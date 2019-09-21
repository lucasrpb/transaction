package transaction

import java.util.concurrent.atomic.AtomicLong
import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import transaction.protocol._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class DataPartition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc2")

  var coordinators = Map.empty[String, Service[Command, Command]]

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val READ_BATCH = session.prepare("select * from batches where id=?;")
  val INCREMENT_BATCH = session.prepare("update batches set n = n + 1 where id=?;")
  val COMPLETE_BATCH = session.prepare("update batches set completed=true, conflicted=?, applied=? where id=?;")
  val READ_EPOCH = session.prepare("select * from log where offset=?;")

  def getBatch(id: String): Future[Batch] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      Any.parseFrom(rs.one.getBytes("bin").array()).unpack(Batch)
    }
  }

  def readBatch(id: String): Future[Int] = {
    session.executeAsync(READ_BATCH.bind.setString(0, id)).map { rs =>
      rs.one.getInt("n")
    }
  }

  def readKey(k: String, v: MVCCVersion, tx: String): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map{rs =>
      val one = rs.one()
      one != null && one.getString("version").equals(v.version) || one.getString("version").equals(tx)}
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction, txs: Seq[Transaction]): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r, txs.find(_.ws.exists(x => x.k.equals(r.k))).get.id)})
      .map(!_.contains(false))
  }

  val wb = new BatchStatement()

  def writeTx(t: Transaction): Future[Boolean] = {
    wb.clear()

    t.ws.foreach { x =>
      val k = x.k
      val v = x.v
      val version = x.version

      if((k.toInt % DataPartitionMain.n) == eid){
        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        if((k.toInt % DataPartitionMain.n) == eid){
          wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
        }
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def increment(id: String): Future[Boolean] = {
    session.executeAsync(INCREMENT_BATCH.bind.setString(0, id)).map(_.wasApplied())
  }

  def complete(id: String, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    session.executeAsync(COMPLETE_BATCH.bind.setSet(0, conflicted.toSet.asJava)
      .setSet(1, applied.toSet.asJava).setString(2, id)).map(_.wasApplied())
  }

  def sendToCoordinator(b: Batch, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    if(!id.equals(b.partitions.keys.toSeq.sorted.head)){
      return Future.value(true)
    }

    println(s"partition ${id} sending ack to coordinator ${b.coordinator}\n")

    val coord = coordinators(b.coordinator)

    complete(b.id, conflicted, applied).map { ok =>
      coord(CoordinatorResult(id, conflicted, applied))
      ok
    }
  }

  def run2(b: Batch): Future[Boolean] = {

    println(s"partition ${id} processing batch ${b.id}\n")

    val txs = b.txs
    val partitions = b.partitions

    readBatch(b.id).flatMap { n =>
      if(n == partitions.size){

        println(s"completed batch ${id}\n")

        Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
          val conflicted = reads.filter(_._2 == false).map(_._1)
          val applied = reads.filter(_._2 == true).map(_._1)

          write(applied).flatMap { ok =>
            sendToCoordinator(b, conflicted.map(_.id), applied.map(_.id))
          }
        }

      } else {

        val timer = new Timer()
        val p = Promise[Boolean]

        timer.schedule(new TimerTask {
          override def run(): Unit = {
            run2(b).onSuccess { ok =>
              p.setValue(ok)
            }.onFailure { ex =>
              p.setException(ex)
            }.ensure {
              timer.purge()
            }
          }
        }, 10L)

        p
      }
    }
  }

  def processBatch(bid: String): Future[Boolean] = {
    getBatch(bid).flatMap { b =>

      println(s"partition ${id} read batch ${b.id} \n")

      if(!b.partitions.isDefinedAt(id)){
        Future.value(true)
      } else {
        increment(b.id).flatMap { _ =>
          run2(b)
        }
      }
    }
  }

  def readEpoch(offset: Long): Option[String] = {
    val rs = session.execute(READ_EPOCH.bind.setLong(0, offset))
    val one = rs.one()
    if(one == null) None else Some(one.getString("batch"))
  }

  val pos = new AtomicLong(0L)

  val timer = new java.util.Timer()

  class Job extends TimerTask {
    override def run(): Unit = {

      if(coordinators.isEmpty) coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
        id -> createConnection(host, port)
      }

      val opt = readEpoch(pos.get())

      if(opt.isEmpty) {
        timer.schedule(new Job(), 10L)
        return
      }

      processBatch(opt.get).onSuccess { _ =>
        pos.incrementAndGet()
        timer.schedule(new Job(), 10L)
      }.handle { case ex =>
        ex.printStackTrace()
      }

    }
  }

  timer.schedule(new Job(), 10L)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
