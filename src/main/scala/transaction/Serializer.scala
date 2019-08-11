package transaction

import java.util.concurrent.ConcurrentLinkedQueue
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
import scala.util.{Failure, Success}

class Serializer(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val batch = new ConcurrentLinkedQueue[Transaction]()
  val timer = new Timer()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  //Await.ready(producer.closeFuture(), 60 seconds)

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

      //if(!executing.isEmpty || batch.isEmpty) return

      //Runtime.getRuntime.exec("cls")

      val now = System.currentTimeMillis()
      var txs = Seq.empty[Transaction]

      /*while(!batch.isEmpty){
        txs = txs :+ batch.poll()
      }*/

      val it = batch.iterator()

      while(it.hasNext) txs = txs :+ it.next()

      val executing = new TrieMap[String, Transaction]()
      var keys = Seq.empty[String]

      txs.sortBy(_.id).foreach { t =>
        val elapsed = now - t.tmp

        if(elapsed >= TIMEOUT){

          t.p.setValue(Nack())

          println(s"${t.id} timed out!")

        } else if(!t.e.ws.keys.exists(k => keys.contains(k))) {

          keys = keys ++ t.e.rs
          executing.put(t.id, t)

          println(s"adding ${t.id} to the batch...")
        } else {
          t.p.setValue(Nack())
          println(s"conflicting tx ${t.id}...")
        }

        batch.remove(t)
      }

      if(executing.isEmpty){
        return
      }

      //println(s"txs: ${batch.size()}\n")

      val b = Batch(executing.map(_._2.e).toSeq)

      //println(s"sending batch: ${b}...")

      log(b).map { _ =>
        val ack = Ack()
        executing.map{case (id, t) =>
          t.p.setValue(ack)
        }
      }.handle {
        case t: Throwable =>

          println(s"exception ${t}")

          val nack = Nack()
          executing.map{case (id, t) =>
            t.p.setValue(nack)
          }
      }

    }
  }, 10L, 10L)

  def enqueue(cmd: Enqueue): Future[Command] = {
    val t = Transaction(cmd.id, cmd, System.currentTimeMillis())

    /*if(!executing.isEmpty) {
      t.p.setValue(Nack())
      return t.p
    }*/

    //println(s"batch size: ${batch.size()}\n")

    if(batch.size() < 10000){
      batch.offer(t)
      return t.p
    }

    t.p.setValue(Nack())

    t.p
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Enqueue => enqueue(cmd)
    }
  }
}
