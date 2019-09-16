package transaction

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.{Properties, Timer, TimerTask, UUID}

import com.datastax.driver.core.{Cluster, HostDistance, PoolingOptions}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

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

  val POS = new AtomicLong(id.toLong)

  val INSERT_BATCH = session.prepare("insert into batches(coordinator, pos, bin) values(?,?,?);")
  val INSERT_BATCH_EMPTY = session.prepare("insert into batches(coordinator, pos) values(?,?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()
    val rs = t.rs
    val ws = t.ws
    val partitions = (rs ++ ws).distinct.map(k => (k.toInt % DataPartitionMain.n).toString)
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val executing = TrieMap[String, Request]()

  val timer = new Timer()

  def insert(opt: Option[Batch]): Future[Boolean] = {
    val pos = POS.getAndAdd(CoordinatorMain.n)

    if(opt.isEmpty){
      return session.executeAsync(INSERT_BATCH_EMPTY.bind.setString(0, id).setLong(1, pos)).map(_.wasApplied())
    }

    val b = opt.get

    val buf = ByteBuffer.wrap(Any.pack(b).toByteArray)
    session.executeAsync(INSERT_BATCH.bind.setString(0, id).setLong(1, pos).setBytes(2, buf)).map(_.wasApplied())
  }

  class Job extends TimerTask {
    override def run(): Unit = {

      if(batch.isEmpty){
        insert(None).onSuccess { _ =>
          timer.schedule(new Job(), 10L)
        }

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

      if(txs.isEmpty) {
        insert(None)
        return
      }



      val b = Batch(UUID.randomUUID.toString, partitions, txs.map(_.id), id)

      insert(Some(b)).map { ok =>
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
      }.map { _ =>
        timer.schedule(new Job(), 10L)
      }.handle { case t =>
        t.printStackTrace()

        txs.foreach { r =>
          r.p.setValue(Nack())
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

  def read(key: String): Future[MVCCVersion] = {
    session.executeAsync(READ_DATA.bind.setString(0, key)).map { rs =>
      val one = rs.one()
      MVCCVersion(one.getString("key"), one.getLong("value"), one.getString("version"))
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
        case Some(r) => /*if(!r.p.isDefined)*/ r.p.setValue(Nack())
      }
    }

    pr.applied.foreach { t =>
      executing.get(t) match {
        case None =>
        case Some(r) => /*if(!r.p.isDefined)*/ r.p.setValue(Ack())
      }
    }

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: ReadRequest => process(cmd)
      case cmd: CoordinatorResult => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
