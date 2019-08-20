package transaction

import java.util.{Timer, TimerTask}
import java.util.concurrent.ConcurrentLinkedQueue

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

  val executing = TrieMap.empty[String, Request]

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()

    val rs = t.partitions.values.map(_.rs.map(_._1)).flatten.toSeq
    val ws = t.partitions.values.map(_.ws.map(_._1)).flatten.toSeq

    val total = t.partitions.size
    var n = 0
    var committed = 0
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val timer = new Timer()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  def log(b: Batch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("transactions",
      id, Any.pack(b).toByteArray)

    val p = Promise[Boolean]()

    producer.writeFuture(record).onComplete {
      case Success(r) => p.setValue(true)
      case Failure(e) => p.setException(e)
    }

    p
  }

  timer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {

      val it = batch.iterator()
      var txs = Seq.empty[Request]

      while(it.hasNext){
        txs = txs :+ it.next()
      }

      val now = System.currentTimeMillis()
      var keys = Seq.empty[String]

      txs = txs.sortBy(_.id).filter { r =>

        val elapsed = now - r.tmp
        batch.remove(r)

        if(elapsed >= TIMEOUT){
          r.p.setValue(Nack())
          false
        } else if(!keys.exists{r.ws.contains(_)}) {

          keys = keys ++ r.rs
          executing.put(r.id, r)

          true
        } else {
          r.p.setValue(Nack())
          false
        }
      }

      val b = Batch(id, txs.map(_.id))

      log(b).handle { case ex =>
          txs.foreach { t =>
            t.p.setValue(Nack())
            executing.remove(t.id)
          }
      }
    }
  }, 10L, 10L)

  def process(t: Transaction): Future[Command] = {
    val req = Request(t.id, t)
    batch.offer(req)
    req.p
  }

  def process(r: PartitionResult): Future[Command] = {
    r.conflicted.foreach { id =>
      val t = executing.remove(id).get
      t.p.setValue(Nack())
    }

    r.applied.foreach { id =>
      val t = executing(id)

      t.committed += 1

      if(t.total == t.n){
        if(t.committed == t.n){
          t.p.setValue(Ack())
        } else {
          t.p.setValue(Nack())
        }

        executing.remove(id)
      }
    }

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: PartitionResult => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
