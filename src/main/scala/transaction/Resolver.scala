package transaction

import java.util.{Timer, TimerTask, UUID}
import java.util.concurrent.ConcurrentLinkedDeque

import com.google.protobuf.any.Any
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

  val buffer = new ConcurrentLinkedDeque[Batch]()

  def log(e: Epoch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("log", e.id, Any.pack(e).toByteArray)
    producer.writeFuture(record).map(_ => true)
  }

  val timer = new Timer()

  class Job extends TimerTask {
    override def run(): Unit = {
      if(buffer.isEmpty){
        timer.schedule(new Job(), 10L)
        return
      }

      var batches = Seq.empty[Batch]

      while(!buffer.isEmpty){
        batches = batches :+ buffer.poll()
      }

      val e = Epoch.apply(UUID.randomUUID.toString, batches.map(_.id))

      log(e).ensure {
        if(buffer.isEmpty){
          timer.schedule(new Job(), 10L)
        } else {
          this.run()
        }
      }
    }
  }

  timer.schedule(new Job(), 10L)

  /**
    * Txs that span multiple dcs must be put separately and be logged in all involved dcs :)
    * @param batch
    * @return
    */
  def process(batch: Batch): Future[Command] = {
    buffer.offer(batch)
    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
    }
  }
}
