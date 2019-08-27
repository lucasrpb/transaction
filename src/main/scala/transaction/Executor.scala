package transaction

import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster}
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

  val wb = new BatchStatement()

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

  def write(ws: Seq[(String, VersionedValue)]): Future[Boolean] = {
    val wb = new BatchStatement()

    /*txs.foreach { t =>
      t.ws.foreach { case (k, v) =>
        if((k.toInt % NPARTITIONS) % ExecutorServer.n == eid){
          wb.add(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
        }
      }
    }*/

    ws.foreach { case (k, v) =>
      wb.add(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def process(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {

    val bytes = evt.value()
    val obj = Any.parseFrom(bytes)
    val batch = obj.unpack(Batch)

    val partitions = batch.partitions.filter{case (k, _) => k.toInt % ExecutorServer.n == eid}
    val txs = partitions.map(_._2.txs)
      .toSeq.flatten.distinct.map(batch.transactions(_))

    if(txs.isEmpty){
      println(s"${Console.RED}EXECUTOR ${id} JUMPING BATCH ${batch.id}...${Console.RESET}")
      //consumer.commit()
      return
    }

    val c = coordinators(batch.coordinator)

    println(s"${Console.GREEN}EXECUTOR ${id} PROCESSING BATCH ${batch.id}...${Console.RESET}\n")

    consumer.pause()

    val f = Future.collect(txs.map{t => checkTx(t).map(t -> _)}).flatMap { reads =>

      val conflicted = reads.filter(_._2 == false)
      val nconflicted = reads.filter(_._2 == true)
      val ws = nconflicted.map(_._1.ws).flatten.filter{case (k, _) => (k.toInt % NPARTITIONS) % ExecutorServer.n == eid}

      def checkFinished(): Future[Boolean] = {
        readBatch(batch.id, batch.total).flatMap { ok =>
          if(ok) {
            c.apply(PartitionResponse.apply(id, conflicted.map(_._1.id), nconflicted.map(_._1.id))).map(_ => true)
          } else {

            println(s"TRYING AGAIN...")

            checkFinished()
          }
        }
      }

      write(ws).flatMap { _ =>
        updateBatch(batch.id, batch.total, partitions.size).flatMap { ok =>
          if(ok){
            c.apply(PartitionResponse.apply(id, conflicted.map(_._1.id), nconflicted.map(_._1.id)))
          } else {
            checkFinished()
          }
        }
      }

    }.handle { case t =>
      t.printStackTrace()
    }.ensure {

    }

    Await.ready(f)

    consumer.resume()
    consumer.commit()

  }

  consumer.handler(process)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
