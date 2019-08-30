package transaction

import java.util.UUID

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Scheduler(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val consumerConfig = scala.collection.mutable.Map[String, String]()

  consumerConfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  consumerConfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  consumerConfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  consumerConfig += (ConsumerConfig.GROUP_ID_CONFIG -> s"partition_${id}")
  consumerConfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  consumerConfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val producerConfig = scala.collection.mutable.Map[String, String]()

  producerConfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  producerConfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  producerConfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  producerConfig += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, consumerConfig)
  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, producerConfig)

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer ${id} subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  def log(e: Epoch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("epoch", id, Any.pack(e).toByteArray)
    producer.writeFuture(record).map(_ => true)
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
      new String(e.value())
    })

    consumer.pause()

    log(epoch).flatMap { _ =>
      Future.collect(partitions.map{case (_, s) => s(epoch)}.toSeq)
    }.handle { case t =>
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
