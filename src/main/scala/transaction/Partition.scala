package transaction

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Partition(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  lazy val coordinators: Map[String, Service[Command, Command]] = CoordinatorMain.coordinators.map { case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val vertx = Vertx.vertx()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"partition_$id")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Partition ${id} subscribed")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  val batches = TrieMap[String, BatchInfo]()
  val executing = TrieMap[String, BatchInfo]()

  def handler(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    val e = Any.parseFrom(evt.value()).unpack(Epoch)

    println(s"partition ${id} processing epoch ${e.id} with batches ${e.batches.map(_.id)}\n")

    e.batches.filter(_.partitions.contains(id)).foreach { b =>
      batches.put(b.id, b)
    }

    if(batches.isEmpty){
      consumer.commit()
      return
    }

    consumer.pause()

    val bts = batches.values.toSeq.sortBy(_.id)
    var parts = Seq.empty[String]
    var list = Seq.empty[BatchInfo]

    bts.foreach { b =>
      if(!b.partitions.exists{parts.contains(_)}){
        executing.put(b.id, b)
        list = list :+ b
        parts = parts ++ b.partitions
      }

    }

    println(s"sending batch_start to ${list.map(_.id)}\n")

    list.foreach { b =>
      coordinators(b.coordinator)(BatchStart(b.id, id))
    }
  }

  consumer.handler(handler)

  def process(cmd: BatchDone): Future[Command] = synchronized {

    println(s"batch_done ${cmd.id}\n")

    batches.remove(cmd.id)
    executing.remove(cmd.id)

    if(batches.isEmpty){
      consumer.resume()
      consumer.commit()
      return Future.value(Ack())
    }

    val bts = batches.values.toSeq.sortBy(_.id)
    var parts = executing.map(_._2.partitions).flatten.toSeq
    var list = Seq.empty[BatchInfo]

    bts.foreach { b =>
      if(!b.partitions.exists{parts.contains(_)}){
        executing.put(b.id, b)
        list = list :+ b
        parts = parts ++ b.partitions
      }
    }

    list.foreach { b =>
      coordinators(b.coordinator)(BatchStart(b.id, id))
    }

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: BatchDone => process(cmd)
    }
  }
}
