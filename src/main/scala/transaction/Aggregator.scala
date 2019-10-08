package transaction

import java.util.UUID

import com.google.protobuf.any.Any
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

  val pconfig = scala.collection.mutable.Map[String, String]()

  pconfig += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  pconfig += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  pconfig += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  pconfig += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, pconfig)

  val cconfig = scala.collection.mutable.Map[String, String]()

  cconfig += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  cconfig += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  cconfig += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  cconfig += (ConsumerConfig.GROUP_ID_CONFIG -> s"aggregator")
  cconfig += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  cconfig += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, cconfig)

  /*val partition = new TopicPartition(new io.vertx.kafka.client.common.TopicPartition("log", id.toInt))
  consumer.assign(partition)*/

  consumer.subscribeFuture("batches").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
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
      Any.parseFrom(evts.recordAt(i).value()).unpack(BatchInfo)
    }

    val e = Epoch(UUID.randomUUID.toString, batches)

    log(e).handle { case ex =>
      ex.printStackTrace()
    }.ensure {
      consumer.commit()
    }
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handler)
}
