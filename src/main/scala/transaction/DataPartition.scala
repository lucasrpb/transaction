package transaction

import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

class DataPartition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
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

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val READ_BATCH = session.prepare("select * from batches where id=?;")
  val INCREMENT_BATCH = session.prepare("update batches set n = n + 1 where id=?;")
  val COMPLETE_BATCH = session.prepare("update batches set completed=true, aborted=?, applied=? where id=?;")

  def getBatch(id: String): Future[Batch] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      Any.parseFrom(rs.one.getBytes("bin").array()).unpack(Batch)
    }
  }

  def readBatch(id: String): Future[Int] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      rs.one.getInt("n")
    }
  }

  def readKey(k: String, v: MVCCVersion, tx: String): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map{rs =>
      val one = rs.one()
      one != null && one.getString("version").equals(v.version) || one.getString("version").equals(tx)}
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction, txs: Seq[Transaction]): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r, txs.find(_.ws.exists(x => x.k.equals(r.k))).get.id)})
      .map(!_.contains(false))
  }

  val wb = new BatchStatement()

  def writeTx(t: Transaction): Future[Boolean] = {
    wb.clear()

    t.ws.foreach { x =>
      val k = x.k
      val v = x.v
      val version = x.version

      if((k.toInt % DataPartitionMain.n) == eid){
        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        if((k.toInt % DataPartitionMain.n) == eid){
          wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
        }
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def increment(id: String): Future[Boolean] = {
    session.executeAsync(INCREMENT_BATCH.bind.setString(0, id)).map(_.wasApplied())
  }

  def complete(id: String, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    session.executeAsync(COMPLETE_BATCH.bind.setSet(0, conflicted.toSet.asJava)
      .setSet(1, applied.toSet.asJava).setString(2, id)).map(_.wasApplied())
  }

  def sendToCoordinator(c: String, p: String, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    if(!id.equals(p)){
      return Future.value(true)
    }

    val coord = coordinators(c)

    complete(id, conflicted, applied).map { ok =>
      coord(CoordinatorResult(id, conflicted, applied))
      ok
    }
  }

  def run2(b: Batch): Future[Boolean] = {

    println(s"partition ${id} processing batch ${b.id}\n")

    val c = coordinators(b.coordinator)
    val txs = b.txs

    val partitions = b.partitions

    readBatch(b.id).flatMap { n =>
      if(n == partitions.size){

        Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
          val conflicted = reads.filter(_._2 == false).map(_._1)
          val applied = reads.filter(_._2 == true).map(_._1)

          write(applied).flatMap { ok =>
            sendToCoordinator(b.coordinator, b.partitions.keys.toSeq.sorted.head, conflicted.map(_.id), applied.map(_.id))
          }
        }

      } else {
        val timer = new Timer()
        val p = Promise[Boolean]

        timer.schedule(new TimerTask {
          override def run(): Unit = {
            timer.purge()

            run2(b).onSuccess { ok =>
              p.setValue(ok)
            }.onFailure { ex =>
              p.setException(ex)
            }
          }
        }, 10L)

        p
      }
    }
  }

  def processBatch(id: String): Future[Boolean] = {
    getBatch(id).flatMap { b =>
      if(!b.partitions.isDefinedAt(id)){
        Future.value(true)
      } else {
        increment(id).flatMap { _ =>
          run2(b)
        }
      }
    }
  }

  def handle(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    val epoch = Any.parseFrom(evt.value()).unpack(Epoch)

    Future.traverseSequentially(epoch.batches) { id =>
      processBatch(id)
    }.onSuccess { _ =>
      consumer.commit()
    }
  }

  consumer.handler(handle)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
