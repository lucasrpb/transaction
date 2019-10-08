package transaction

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Scheduler()(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val vertx = Vertx.vertx()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"aggregator")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  lazy val workers: Map[String, Service[Command, Command]] = WorkerMain.workers.map { case (id, (host, port)) =>
      id -> createConnection(host, port)
  }

  def handler(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    val e = Any.parseFrom(evt.value()).unpack(Epoch)

    val w = workers("0")

    Future.traverseSequentially(e.batches) { b =>
      w(b)
    }.ensure {
      consumer.commit()
    }
  }

  consumer.handler(handler)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
