package transaction

import java.util.TimerTask
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class DataPartition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

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

  var PARTITIONS = Map.empty[String, Service[Command, Command]]
  val BATCHES = createConnection("127.0.0.1", 5000)

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val READ_OFFSET = session.prepare("select offset from offsets where id=?;")
  val UPDATE_OFFSET = session.prepare("update offsets set offset = offset + 1 where id=?;")

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

  def run2(b: Batch, offset: Int): Future[Unit] = {

    println(s"partition ${id} processing batch ${b.id}\n")

    val c = coordinators(b.coordinator)
    val txs = b.transactions

    val partitions = b.partitions//.filterNot(_._2.id.equals(id))

    BATCHES.apply(GetBatch(b.id)).flatMap { r =>
      val res = r.asInstanceOf[GetBatchResponse].count

      if(res == partitions.size){

        Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
          val conflicted = reads.filter(_._2 == false).map(_._1)
          val applied = reads.filter(_._2 == true).map(_._1)

          write(applied).flatMap { ok =>
            c(PartitionResponse(id, conflicted.map(_.id), applied.map(_.id)))
          }.map { _ =>
            consumer.commit()
            consumer.resume()
            Ack()
          }
        }

      } else {
        run2(b, 0)
      }

    }
  }

  def handle(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {

    if(PARTITIONS.isEmpty) PARTITIONS = DataPartitionMain.partitions.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val b = Any.parseFrom(evt.value()).unpack(Batch)

    val partitions = b.partitions

    if(!partitions.isDefinedAt(id)){
      consumer.commit()
      return
    }

    consumer.pause()

    BATCHES(IncBatch(b.id)).flatMap { _ =>
      run2(b, 0)
    }
  }

  consumer.handler(handle)

  def process(value: RequestOffset): Future[Command] = {
    Future.value(ResponseOffset(0L))
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: RequestOffset => process(cmd)
    }
  }
}
