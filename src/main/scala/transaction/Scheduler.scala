package transaction

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Scheduler()(implicit val ec: ExecutionContext){

  var workers = Map.empty[String, Service[Command, Command]]

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"scheduler")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  def handle(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {

    if(workers.isEmpty) workers = WorkerMain.workers.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val batches = (0 until evts.size).map { idx =>
      Any.parseFrom(evts.recordAt(idx).value()).unpack(Batch)
    }

    val w = workers.head._2

    Future.traverseSequentially(batches) { b =>
      w.apply(b)
    }.handle { case ex =>
      ex.printStackTrace()
    }.ensure {
      consumer.commit()
    }
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handle)
}
