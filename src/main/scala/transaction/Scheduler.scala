package transaction

import java.util.UUID

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.core.{AsyncResult, Handler}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol.{Batch, Epoch}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Scheduler()(implicit val ec: ExecutionContext){

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
    case Failure(cause) => println("Failure")
  }

  var partitions = Map.empty[String, Service[Command, Command]]

  def process(e: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    if(partitions.isEmpty) partitions = DataPartitionMain.partitions.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val batches = (0 until e.size()).map(e.recordAt(_))

    if(batches.isEmpty) {
      consumer.commit()
      return
    }

    println(s"${Console.RED}processing epoch with size ${e.size()}...${Console.RESET}\n")

    val epoch = Epoch(UUID.randomUUID.toString, batches.map { e =>
      Any.parseFrom(e.value()).unpack(Batch)
    })

    consumer.pause()

    Future.collect(partitions.map{_._2.apply(epoch)}.toSeq)
      .ensure {
        consumer.commit()
        consumer.resume()
      }
  }

  consumer.handler(_ => {})
  consumer.batchHandler(process)
}
