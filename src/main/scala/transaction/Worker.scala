package transaction

import com.datastax.driver.core.Cluster
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Worker(val id: String)(implicit val ec: ExecutionContext) {

  val eid = id.toInt

  lazy val coordinators: Map[String, Service[Command, Command]] = CoordinatorMain.coordinators.map { case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val vertx = Vertx.vertx()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"worker_${id}")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  /*val partition = new io.vertx.kafka.client.common.TopicPartition()
    partition.setTopic("batches")
    .setPartition(eid)

  consumer.assign(TopicPartition(partition))*/

  consumer.subscribeFuture("batches").onComplete {
    case Success(result) => {
      println(s"worker ${id} subscribed!")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mv2pl")



  def handler(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    if(evt.partition() != eid) {
      consumer.commit()
      return
    }

    val b = Any.parseFrom(evt.value()).unpack(Batch)

    println(s"processing batch ${b.id}...\n")

    consumer.pause()

    coordinators(b.coordinator)(CoordinatorResult(id, Seq(), b.txs.map(_.id)))
      .handle { case ex =>
        ex.printStackTrace()
      }.ensure {
        consumer.commit()
        consumer.resume()
      }
  }

  consumer.handler(handler)

}
