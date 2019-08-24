package transaction

import java.util.{Timer, TimerTask}

import com.datastax.driver.core.Cluster
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Executor(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val partitions = (0 until NPARTITIONS).filter(_ % ExecutorServer.n == eid).map(_.toString)

  //println(s"executor $id partitions ${partitions}\n")

  val coordinators = CoordinatorServer.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val READ_BATCH = session.prepare("select * from batches where id=?;")
  //val UPDATE_BATCH = session.prepare("update batches set n = n + ? where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def readBatch(id: String, total: Int): Future[Boolean] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      rs.one.getInt("n") == total
    }
  }

  def updateBatch(id: String, total: Int, n: Int): Future[Boolean] = {
    //session.executeAsync(UPDATE_BATCH.bind.setInt(0, n).setString(1, id)).map(_.one.getInt("n") == total)
    val q = s"update batches set n = n + ${n} where id='${id}';"
    session.executeAsync(q).map { rs =>
      val one = rs.one()
      one != null && one.getInt("n") == total
    }
  }

  def readKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map{rs =>
      val one = rs.one()
      one != null && one.getString("version").equals(v.version)}
  }

  def writeKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{case (k, v) => readKey(k, v)}.toSeq).map(!_.contains(false))
  }

  def writeTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.ws.map{case (k, v) => writeKey(k, v)}.toSeq).map(!_.contains(false))
  }

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"partition_${id}")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("transactions").onComplete {
    case Success(result) => {
      println(s"Consumer ${id} subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  def checkBatchFinished(id: String, total: Int, c: Service[Command, Command],
                         conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    val timer = new Timer()
    val p = Promise[Boolean]()

    timer.schedule(new TimerTask {
      override def run(): Unit = {

        println(s"TRYING AGAIN...")

        readBatch(id, total).onSuccess { ok =>
          if(ok){
            sendOkToCoordinator(c, conflicted, applied)
            p.setValue(true)
          } else {
            timer.purge()
            checkBatchFinished(id, total, c, conflicted, applied)
          }
        }
      }
    }, 10L)

    p
  }

  def sendOkToCoordinator(c: Service[Command, Command], conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    c.apply(PartitionResponse(id, conflicted, applied))

    println(s"executor ${id} finished processing")

    consumer.resume()
    consumer.commit()
    Future.value(true)
  }

  def process(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    val bytes = evt.value()
    val obj = Any.parseFrom(bytes)
    val batch = obj.unpack(Batch)

    val partitions = batch.partitions.filter{case (k, _) => k.toInt % ExecutorServer.n == eid}

    if(partitions.isEmpty) {
      println(s"${Console.RED} EXECUTOR ${id} JUMPING batch ${batch.id} TO THE NEXT...${Console.RESET}\n")
      consumer.commit()
      return
    }

    consumer.pause()
    println(s"executor ${id} started processing batch ${batch.id}\n")

    val txs = partitions.map(_._2.txs).flatten.toSeq.distinct.map(batch.transactions(_))

    //println(s"processing txs ${txs.map(_.id)}...\n")

    val c = coordinators(batch.coordinator)

    //println(s"executor ${id} processing batch ${batch.id}\n")

    Future.collect(txs.map{t => checkTx(t).map(t -> _)}).flatMap { reads =>

      val conflicted = reads.filter(_._2 == false)
      val nconflicted = reads.filter{case (t, ok) => ok && t.ws.exists{case (k, _) => k.toInt % ExecutorServer.n == eid}}

      val n = partitions.size

      //println(s"conflicted ${conflicted.map(_._1.id)} not conflicted ${nconflicted.map(_._1.id)}\n")

      Future.collect(nconflicted.map{case (t, _) => writeTx(t)}).flatMap { writes =>

        updateBatch(batch.id, batch.total, n).flatMap { ok =>

          println(s"update batch ${id} => ${ok}\n")

          if(ok){
            sendOkToCoordinator(c, conflicted.map(_._1.id), nconflicted.map(_._1.id))
          } else {
            checkBatchFinished(batch.id, batch.total, c, conflicted.map(_._1.id), nconflicted.map(_._1.id))
          }
        }
      }
    }.handle { case t =>
      t.printStackTrace()
    }

  }

  consumer.handler(process)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
