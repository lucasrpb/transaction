package transaction

import java.util.UUID

import com.google.protobuf.any.Any
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol.{BatchInfo, Epoch}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Aggregator()(implicit val ec: ExecutionContext){

  val vertx = Vertx.vertx()

  val configp = scala.collection.mutable.Map[String, String]()

  configp += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  configp += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  configp += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  configp += (ProducerConfig.ACKS_CONFIG -> "1")

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, configp)

  val configc = scala.collection.mutable.Map[String, String]()

  configc += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  configc += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  configc += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  configc += (ConsumerConfig.GROUP_ID_CONFIG -> s"aggregator")
  configc += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  configc += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, configc)

  consumer.subscribeFuture("batches").onComplete {
    case Success(result) => {
      println(s"Aggregator subscribed")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  def log(e: Epoch): Future[Boolean] = {
    val buf = Any.pack(e).toByteArray
    val record = KafkaProducerRecord.create[String, Array[Byte]]("log", e.id, buf)
    producer.writeFuture(record).map(_ => true)
  }

  def handler(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {
    val batches = (0 until evts.size()).map { i =>
      val bytes = evts.recordAt(i).value()
      Any.parseFrom(bytes).unpack(BatchInfo)
    }

    val e = Epoch(UUID.randomUUID.toString, batches.sortBy(_.id))

    println(s"creating epoch ${e.id} with batches ${e.batches}\n")

    log(e).handle{ case ex =>
      ex.printStackTrace()
    }.ensure {
      consumer.commit()
    }
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handler)
}
