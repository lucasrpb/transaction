package transaction

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask, UUID}
import java.util.concurrent.ConcurrentLinkedDeque

import com.datastax.driver.core.{BatchStatement, Cluster, HostDistance, PoolingOptions}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

  val eid = id.toInt

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

  val session = cluster.connect("mv2pl")

  val INSERT_BATCH = session.prepare("insert into batches(id, status) values(?,?);")
  val READ = session.prepare("select * from data where key=?;")

  val batch = new ConcurrentLinkedDeque[Request]()
  //val batches = TrieMap[String, (Batch, ConcurrentLinkedDeque[String])]()
  val executing = TrieMap[String, Request]()

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()
    val rs = t.rs.map(_.k)
    val ws = t.ws.map(_.k)
    val keys = (rs ++ ws).distinct
    val partitions = keys.map(k => accounts.computeHash(k).toString)
  }

  def save(b: Batch): Future[Boolean] = {
    //batches.put(b.id, b -> new ConcurrentLinkedDeque[String]())
    session.executeAsync(INSERT_BATCH.bind.setString(0, b.id).setInt(1, Status.PENDING)).map(_.wasApplied())
  }

  def log(b: Batch): Future[Boolean] = {
    val buf = Any.pack(b).toByteArray
    val record = KafkaProducerRecord.create[String, Array[Byte]]("batches", b.id, buf)
    producer.writeFuture(record).map(_ => true)
  }

  def process(t: Transaction): Future[Command] = {
    val req = Request(t.id, t)
    batch.offer(req)
    req.p
  }

  def read(key: String): Future[MVCCVersion] = {
    session.executeAsync(READ.bind.setString(0, key)).map { rs =>
      val one = rs.one()
      MVCCVersion(one.getString("key"), one.getLong("value"), one.getString("version"))
    }
  }

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=? and version=?;")
  val READ_BATCH = session.prepare("select * from batches where id=?;")

  def readKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k).setString(1, v.version)).map{_.one() != null}
  }

  def readBatch(id: String): Future[Batch] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      Any.parseFrom(rs.one.getBytes("bin").array()).unpack(Batch)
    }
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k)).map{_.wasApplied()}
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r)}).map(!_.contains(false))
  }

  def write(t: Transaction): Future[Boolean] = {
    val wb = new BatchStatement()

    t.ws.foreach { x =>
      val k = x.k
      val v = x.v
      val version = x.version

      wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
    }

    session.executeAsync(wb).map(_.wasApplied()).handle { case e =>
      e.printStackTrace()
      false
    }
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    val wb = new BatchStatement()

    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied()).handle { case e =>
      e.printStackTrace()
      false
    }
  }

  def process(r: ReadRequest): Future[Command] = {
    Future.collect(r.keys.map{read(_)}).map(r => ReadResponse(r))
  }

  def process(pr: CoordinatorResult): Future[Command] = {
    println(s"conflicted ${pr.conflicted}")
    println(s"applied ${pr.applied}\n")

    pr.conflicted.foreach { t =>
      executing.get(t) match {
        case None =>
        case Some(r) =>
          executing.remove(t)
          r.p.setValue(Nack())
      }
    }

    pr.applied.foreach { t =>
      executing.get(t) match {
        case None =>
        case Some(r) =>
          executing.remove(t)
          r.p.setValue(Ack())
      }
    }

    Future.value(Ack())
  }

  def computePartition(k: String): String = {
    (accounts.computeHash(k).abs % PARTITIONS).toString
  }

  val timer = new Timer()

  class Job() extends TimerTask {
    override def run(): Unit = {

      if(batch.isEmpty){
        timer.schedule(new Job(), 10L)
        return
      }

      val now = System.currentTimeMillis()

      var txs = Seq.empty[Request]

      while(!batch.isEmpty){
        txs = txs :+ batch.poll()
      }

      var keys = Seq.empty[String]

      txs = txs.sortBy(_.id).filter { r =>
        val elapsed = now - r.tmp

        if(elapsed >= TIMEOUT){
          r.p.setValue(Nack())
          false
        } else if(!r.keys.exists{keys.contains(_)}){
          keys = keys ++ r.keys
          true
        } else {
          r.p.setValue(Nack())
          false
        }
      }

      if(txs.isEmpty){
        timer.schedule(new Job(), 10L)
        return
      }

      val partitions = txs.map(r => r.keys).flatten.distinct.map(k => computePartition(k)).distinct.sorted
      val b = Batch(UUID.randomUUID.toString, txs.map(_.t), partitions, id)

      save(b).flatMap(ok => log(b).map(_ && ok)).map { ok =>
        if(ok){
          txs.foreach { r =>
            executing.put(r.id, r)
          }
        } else {
          txs.foreach { r =>
            r.p.setValue(Nack())
          }
        }
      }.handle { case ex =>
        ex.printStackTrace()

        txs.foreach { r =>
          r.p.setValue(Nack())
        }
      }.ensure {
        timer.schedule(new Job(), 10L)
      }
    }
  }

  timer.schedule(new Job(), 10L)

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: ReadRequest => process(cmd)
      case cmd: CoordinatorResult => process(cmd)
    }
  }
}
