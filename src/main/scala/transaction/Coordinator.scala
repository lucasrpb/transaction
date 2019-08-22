package transaction

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask, UUID}
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

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()

    val rs = t.rs.map(_._1)
    val ws = t.ws.map(_._2)
  }

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  val INSERT_TRANSACTION = session.prepare("insert into transactions(id, status, tmp, bin) values(?,?,?, ?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def read(k: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map { r =>
      val one = r.one()
      k -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def insertTx(tx: Transaction): Future[Boolean] = {
    val tmp = System.currentTimeMillis()
    val bytes = Any.pack(tx).toByteArray

    session.executeAsync(INSERT_TRANSACTION.bind.setString(0, tx.id).setInt(1, Status.PENDING).setLong(2, tmp)
      .setBytes(3, ByteBuffer.wrap(bytes))).map { rs => rs.wasApplied()
    }.handle { case t =>

      t.printStackTrace()

      false
    }
  }

  def log(b: Batch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("transactions", id, Any.pack(b).toByteArray)

    val p = Promise[Boolean]()

    producer.writeFuture(record).onComplete {
      case Success(r) => p.setValue(true)
      case Failure(e) => p.setException(e)
    }

    p
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val executing = TrieMap[String, Request]()

  val timer = new Timer()

  timer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {

      var txs = Seq.empty[Request]

      while(!batch.isEmpty){
        txs = txs :+ batch.poll()
      }

      val now = System.currentTimeMillis()

      var keys = Seq.empty[String]

      txs = txs.sortBy(_.id).filter { r =>
        val elapsed = now - r.tmp

        if(elapsed >= TIMEOUT){
          r.p.setValue(Nack())
          false
        } else if(!r.rs.exists(keys.contains(_))){
          keys = keys ++ r.rs
          executing.put(r.id, r)
          true
        } else {
          r.p.setValue(Nack())
          false
        }
      }

      if(txs.isEmpty) return

      val b = Batch.apply(UUID.randomUUID.toString, id, txs.map(_.t))

      log(b).map { ok =>
        if(!ok){
          txs.foreach { t =>
            executing.remove(t.id).get.p.setValue(Nack())
          }
        }
      }.handle { case t =>
        t.printStackTrace()

        txs.foreach { t =>
          executing.remove(t.id).get.p.setValue(Nack())
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

    println(s"coordinator ${id} received result ${r}")

    r.aborted.foreach { t =>
      executing.remove(t).get.p.setValue(Nack())
    }

    r.applied.foreach { t =>
      executing.remove(t).get.p.setValue(Ack())
    }

    Future.value(Ack())
  }

  def process(r: Read): Future[Command] = {
    Future.collect(r.keys.map{k => read(k)}).map(result => ReadResult(result.toMap))
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: PartitionResult => process(cmd)
      case cmd: Read => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
