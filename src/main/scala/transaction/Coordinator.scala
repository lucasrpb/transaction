package transaction

import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentLinkedQueue

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()
  }

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  /*def log(b: Batch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("transactions",
      id, Any.pack(b).toByteArray)

    val p = Promise[Boolean]()

    producer.writeFuture(record).onComplete {
      case Success(r) => p.setValue(true)
      case Failure(e) => p.setException(e)
    }

    p
  }*/

  def log(t: Transaction): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("transactions",
      id, Any.pack(t).toByteArray)

    val p = Promise[Boolean]()

    producer.writeFuture(record).onComplete {
      case Success(r) => p.setValue(true)
      case Failure(e) => p.setException(e)
    }

    p
  }

  val executing = TrieMap[String, Request]()

  def process(t: Transaction): Future[Command] = {
    val req = Request(t.id, t)

    log(t).onSuccess { ok =>
      executing.put(req.id, req)
    }.handle { case t =>
      req.p.setValue(Nack())
    }

    req.p
  }

  def process(r: PartitionResult): Future[Command] = {

    val t = executing(r.id)

    if(r.applied){
      t.p.setValue(Ack())
    } else {
      t.p.setValue(Nack())
    }

    executing.remove(id)

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: PartitionResult => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
