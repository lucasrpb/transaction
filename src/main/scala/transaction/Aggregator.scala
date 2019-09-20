package transaction

import java.util.UUID

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Aggregator()(implicit val ec: ExecutionContext){

  val cconfig = scala.collection.mutable.Map[String, String]()

  cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> s"aggregator")
  cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val pconfig = scala.collection.mutable.Map[String, String]()

  pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  pconfig += (ProducerConfig.BATCH_SIZE_CONFIG -> "16384")
  pconfig += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  val consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)
  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

  consumer.subscribeFuture("batches").onComplete {
    case Success(result) => {
      println(s"Aggregator subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  def log(e: Epoch): Future[Boolean] = {
    val buf = Any.pack(e).toByteArray
    val record = KafkaProducerRecord.create[String, Array[Byte]]("log", e.id, buf)
    producer.writeFuture(record).map(_ => true)
  }

  def handle(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    val batches = (0 until evts.size()).map {i => new String(evts.recordAt(i).value)}

    //println(s"creating batch ${epoch.id} with batches ${batches}\n")

    batches.sorted.foreach { id =>
      val record = KafkaProducerRecord.create[String, Array[Byte]]("log", id, id.getBytes)
      producer.write(record)
    }

    producer.flush(_ => {
      consumer.commit()
    })
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handle)
}
