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

    println(s"sets: rs ${rs} ws: ${ws}\n")

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

      if(batch.isEmpty) return

      val it = batch.iterator()
      var txs = Seq.empty[Request]

      while(it.hasNext){
        txs = txs :+ it.next()
      }

      val now = System.currentTimeMillis()
      //var keys = Seq.empty[String]
      var filter = Seq.empty[Request]

      txs.sortBy(_.id).foreach { r =>

        val elapsed = now - r.tmp
        batch.remove(r)

        if(elapsed >= TIMEOUT){
          r.p.setValue(Nack())
        } else if(!filter.exists{r1 => r.ws.exists(r1.rs.contains(_)) || r1.ws.exists(r.rs.contains(_))}) {
          filter = filter :+ r
        } else {
          r.p.setValue(Nack())
        }
      }

      var j = 0

      filter = filter.filter { r =>

        if(j == 0){
          j += 1
          true
        } else {
          j += 1
          r.p.setValue(Nack())
          false
        }
      }

      val b = Batch(id, filter.map(_.id))

     // println(s"sending batch ${b.transactions}...\n")

      log(b).map { ok =>
        if(ok){

          println(s"${Console.YELLOW}sending batch ${txs.map(r => (r.ws ++ r.ws).distinct)}${Console.RESET}\n")

          filter.foreach { t =>
            executing.put(t.id, t)
          }
        } else {
          filter.foreach { t =>
            t.p.setValue(Nack())
          }
        }

        ok
      }.handle { case ex =>
        filter.foreach { t =>
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

    println(s"received partition result: ${r.conflicted} ${r.applied}\n")

    r.conflicted.foreach { id =>
      val t = executing.remove(id).get
      t.n += 1
      t.p.setValue(Nack())
    }

    r.applied.foreach { id =>
      val t = executing(id)
      t.committed += 1
      t.n += 1
    }

    val remove = executing.filter { case (id, t) =>

      println(s"verifying for $id total ${t.total} committed ${t.committed} n ${t.n}")

      if(t.total == t.n){

        println(s"all completed for ${id}...")

        if(t.committed == t.total){

          println(s"committed!")

          t.p.setValue(Ack())
        } else {

          println(s"not committed")
          t.p.setValue(Nack())
        }

        true
      } else {
        false
      }
    }.map(_._1)

    remove.map(executing.remove(_))

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
