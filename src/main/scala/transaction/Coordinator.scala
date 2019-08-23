package transaction

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Timer, TimerTask}

import com.datastax.driver.core.Cluster
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()

    val rs: Seq[String] = t.rs.map(_._1).toSeq
    val ws: Seq[String] = t.ws.map(_._1).toSeq

    val partitions = (rs ++ ws).distinct.map(k => (k.toInt % NPARTITIONS).toString)
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val executing = TrieMap[String, Request]()

  val timer = new Timer()
  val done = new AtomicBoolean(true)

  def log(b: Batch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("transactions", id, Any.pack(b).toByteArray)
    producer.writeFuture(record).map(_ => true)
  }

  timer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {

      val now = System.currentTimeMillis()

      var txs = Seq.empty[Request]
      val it = batch.iterator()

      while(it.hasNext()){
        txs = txs :+ it.next()
      }

      var keys = executing.map(_._2.rs).flatten.toSeq

      txs.sortBy(_.id).foreach { r =>
        val elapsed = now - r.tmp

        if(elapsed >= TIMEOUT){
          batch.remove(r)
          r.p.setValue(Nack())
        } else if(!r.rs.exists(keys.contains(_))){
          keys = keys ++ r.rs
          batch.remove(r)
          executing.put(r.id, r)
        }
      }

    }
  }, 10L, 10L)

  def process(t: Transaction): Future[Command] = {
    val req = Request(t.id, t)
    batch.offer(req)
    req.p
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
