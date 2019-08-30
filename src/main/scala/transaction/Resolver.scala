package transaction

import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext

class Resolver(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  /**
    * Txs that span multiple dcs must be put separately and be logged in all involved dcs :)
    * @param batch
    * @return
    */
  def process(batch: Batch): Future[Command] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("log", batch.id, batch.id.getBytes)
    producer.writeFuture(record).map(_ => Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
    }
  }
}
