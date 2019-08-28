package transaction

import java.util.UUID

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Processor(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

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

  var partitions = Map.empty[String, Service[Command, Command]]

  def handle(e: KafkaConsumerRecords[String, Array[Byte]]): Unit = {

    if(partitions.isEmpty) partitions = DataPartitionServer.partitions.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val batches = (0 until e.size()).map(e.recordAt(_))

    if(batches.isEmpty) {
      consumer.commit()
      return
    }

    println(s"${Console.RED}processing epoch with size ${e.size()}...${Console.RESET}\n")

    val epoch = Epoch(UUID.randomUUID.toString, batches.map { e =>
      val bytes = e.value()
      val obj = Any.parseFrom(bytes)
      obj.unpack(Batch)
    })

    consumer.pause()

    Future.collect(partitions.map{case (_, s) => s(epoch)}.toSeq)
      .handle { case t =>
        t.printStackTrace()
      }
      .ensure {
      consumer.commit()
      consumer.resume()
    }
  }

  consumer.handler((_) => {})

  consumer.batchHandler(handle)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
