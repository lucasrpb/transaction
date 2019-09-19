package transaction

import java.util.concurrent.atomic.AtomicLong
import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster, Row}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class DataPartition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  var PARTITIONS = Map.empty[String, Service[Command, Command]]

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val READ_OFFSET = session.prepare("select offset from offsets where id=?;")
  val UPDATE_OFFSET = session.prepare("update offsets set offset = offset + 1 where id=?;")
  val GET_BATCH = session.prepare("select * from batches where coordinator=?;")
  val INC_BATCH = session.prepare("update batches set n = n + 1 where coordinator=?;")
  val UPDATE_BATCH = session.prepare("update batches set completed = true, aborted = ?, applied = ? where coordinator=?;")

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

  def updateBatch(c: String, pos: Long, size: Int): Future[Boolean] = {
    session.executeAsync(INC_BATCH.bind.setString(0, s"$c-$pos")).map(_.wasApplied())
  }

  def getBatch(c: String, pos: Long): Future[Option[Batch]] = {
    session.executeAsync(GET_BATCH.bind.setString(0, s"$c-$pos")).map { r =>
      val one = r.one()

      if(one == null) {
        None
      } else {
        val bytes = one.getBytes("bin")
        if(bytes == null) Some(null) else Some(Any.parseFrom(r.one.getBytes("bin").array()).unpack(Batch))
      }
    }
  }

  def readBatch(c: String, pos: Long): Future[Row] = {
    session.executeAsync(GET_BATCH.bind.setString(0, s"$c-$pos")).map { rs =>
      rs.one
    }
  }

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

  def completeBatch(c: String, pos: Long, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    session.executeAsync(UPDATE_BATCH.bind.setSet(0, conflicted.toSet.asJava)
      .setSet(1, applied.toSet.asJava).setString(2, id)).map(_.wasApplied())
  }

  def sendToCoordinator(b: Batch, c: String, pos: Long,
                        conflicted: Seq[String], applied: Seq[String], isLeader: Boolean): Future[Boolean] = {
    if(!isLeader) return Future.value(true)
    val coord = coordinators(b.coordinator)
    completeBatch(c, pos, conflicted, applied).map { ok =>
      coord(CoordinatorResult.apply(id, conflicted, applied))
      ok
    }
  }

  def run2(b: Batch, c: String, pos: Long): Future[Boolean] = {
    println(s"partition ${id} processing batch ${b.id}\n")

    val partitions = b.partitions
    val txs = b.txs

    readBatch(c, pos).flatMap { data =>

      val n = data.getInt("n")
      val leader = data.getString("leader")

      if(n == partitions.size){
        Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
          val conflicted = reads.filter(_._2 == false).map(_._1)
          val applied = reads.filter(_._2 == true).map(_._1)

          write(applied).flatMap { _ =>
            sendToCoordinator(b, c, pos, conflicted.map(_.id), applied.map(_.id), leader.equals(id))
          }
        }
      } else {
        run2(b, c, pos)
      }
    }
  }

  def increment(b: Batch, c: String, pos: Long): Future[Boolean] = {
    val partitions = b.partitions

    if(!partitions.isDefinedAt(id)){
      return Future.value(true)
    }

    updateBatch(c, pos, partitions.size).flatMap { ok =>
      run2(b, c, pos)
    }
  }

  val timer = new Timer()

  class Job() extends TimerTask {
    override def run(): Unit = {

      val pos = POS.get()
      val c = (pos % CoordinatorMain.n).toString

      //println(s"processing position $pos at partition $id...\n")

      if(PARTITIONS.isEmpty) PARTITIONS = DataPartitionMain.partitions.map{ case (id, (host, port)) =>
        id -> createConnection(host, port)
      }

      getBatch(c, pos).flatMap { opt =>
        opt match {
          case None =>
            Future.value(true)
          case Some(b) =>

            if(b == null){
              POS.incrementAndGet()
              Future.value(false)
            } else {
              println(s"processing position $pos at partition $id...\n")

              increment(b, c, pos).map { ok =>
                POS.incrementAndGet()
                ok
              }
            }
        }
      }.handle { case t  =>
        t.printStackTrace()
      }.ensure {
        timer.schedule(new Job(), 10L)
      }

    }
  }

  timer.schedule(new Job(), 10L)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
