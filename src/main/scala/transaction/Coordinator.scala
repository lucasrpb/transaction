package transaction

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Timer, TimerTask, UUID}

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions}
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

  val config = scala.collection.mutable.Map[String, String]()

  config += (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
  config += (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArraySerializer")
  config += (ProducerConfig.ACKS_CONFIG -> "1")

  val vertx = Vertx.vertx()

  // use producer for interacting with Apache Kafka
  val producer = KafkaProducer.create[String, Array[Byte]](vertx, config)

  val poolingOptions = new PoolingOptions()
    //.setConnectionsPerHost(HostDistance.LOCAL, 1, 200)
    .setMaxRequestsPerConnection(HostDistance.LOCAL, 3000)
    //.setNewConnectionThreshold(HostDistance.LOCAL, 2000)
    //.setCoreConnectionsPerHost(HostDistance.LOCAL, 2000)

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .withPoolingOptions(poolingOptions)
    .build()

  val session = cluster.connect("mvcc")

  session.execute("truncate batches;")

  val INSERT_BATCH = session.prepare("insert into batches(id, total, n, bin) values(?,?,?,?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()

    val rs: Seq[String] = t.rs.map(_._1).toSeq
    val ws: Seq[String] = t.ws.map(_._1).toSeq

    val partitions = (rs ++ ws).distinct.map(k => (k.toInt % NPARTITIONS).toString)
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val executing = TrieMap[String, Request]()

  val timer = new Timer()

  def insertBatch(b: Batch): Future[Boolean] = {
    session.executeAsync(INSERT_BATCH.bind.setString(0, b.id).setInt(1, b.partitions.size).setInt(2, 0)
      .setBytes(3, ByteBuffer.wrap(Any.pack(b).toByteArray))).map(_.wasApplied())
  }

  def log(b: Batch): Future[Boolean] = {
    val record = KafkaProducerRecord.create[String, Array[Byte]]("log", b.id, b.id.getBytes)
    producer.writeFuture(record).map(_ => true)
  }

  class Job extends TimerTask {
    override def run(): Unit = {

      if(batch.isEmpty){
        timer.schedule(new Job(), 10L)
        return
      }

      val now = System.currentTimeMillis()

      var txs = Seq.empty[Request]
      val it = batch.iterator()

      while(it.hasNext()){
        txs = txs :+ it.next()
      }

      var keys = Seq.empty[String]

      txs = txs.sortBy(_.id).filter { r =>
        val elapsed = now - r.tmp

        batch.remove(r)

        if(elapsed >= TIMEOUT){
          r.p.setValue(Nack())
          false
        } else if(!r.ws.exists(keys.contains(_))){
          keys = keys ++ r.rs
          true
        } else {
          r.p.setValue(Nack())
          false
        }
      }

      if(txs.isEmpty) return

      var partitions = Map[String, Partition]()

      txs.foreach { r =>

        //println(s"partitions ${r.partitions}\n")

        r.partitions.foreach { p =>
          partitions.get(p) match {
            case None => partitions = partitions + (p -> Partition(p, Seq(r.id)))
            case Some(pt) => partitions = partitions + (p -> Partition(p, pt.txs :+ r.id))
          }
        }
      }

      val b = Batch(UUID.randomUUID.toString, id, txs.map(r => r.id -> r.t).toMap, partitions, partitions.size)

      insertBatch(b).flatMap(ok1 => log(b).map(ok1 && _)).map { ok =>
        if(ok) {
          txs.foreach { r =>
            executing.put(r.id, r)
          }
        } else {
          txs.foreach { r =>
            batch.remove(r)
            r.p.setValue(Nack())
          }
        }
      }.handle { case t =>
        t.printStackTrace()

        txs.foreach { r =>
          r.p.setValue(Nack())
        }
      }.ensure {
        if(batch.isEmpty){
          timer.schedule(new Job(), 10L)
        } else {
          this.run()
        }
      }

    }
  }

  timer.schedule(new Job(), 10L)

  def process(t: Transaction): Future[Command] = {
    val req = Request(t.id, t)
    batch.offer(req)
    req.p
  }

  def read(key: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, key)).map { rs =>
      val one = rs.one()
      key -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def process(r: Read): Future[Command] = {
    Future.collect(r.keys.map{read(_)}).map(r => ReadResult(r.toMap))
  }

  def process(pr: PartitionResponse): Future[Command] = {
    println(s"conflicted ${pr.conflicted}")
    println(s"applied ${pr.applied}\n")

    pr.conflicted.foreach { t =>
      executing.get(t) match {
        case None =>
        case Some(r) => if(!r.p.isDefined) r.p.setValue(Nack())
      }
    }

    pr.applied.foreach { t =>
      executing.get(t) match {
        case None =>
        case Some(r) => if(!r.p.isDefined) r.p.setValue(Ack())
      }
    }

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: Read => process(cmd)
      case cmd: PartitionResponse => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
