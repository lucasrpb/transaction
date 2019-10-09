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

  var batch: Option[BatchInfo] = None

  def handler(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = synchronized {
    val b = Any.parseFrom(evt.value()).unpack(BatchInfo)

    if(!b.partitions.exists(_.equals(id))){
      consumer.commit()
      return
    }

    consumer.pause()

    println(s"partition ${id} sending batch ${b.id} to coordinator ${b.coordinator}\n")

    batch = Some(b)

    coordinators(b.coordinator)(BatchStart(b.id, id))
  }

  consumer.handler(handler)

  def process(cmd: BatchDone): Future[Command] = synchronized {
    //if(batch.isEmpty || !batch.get.id.equals(cmd.id)) return Future.value(Ack())

    batch = None
    consumer.commit()
    consumer.resume()

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: BatchDone => process(cmd)
    }
  }
}
