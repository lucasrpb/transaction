package transaction

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.concurrent.ExecutionContext

class Worker(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  var coordinators = Map.empty[String, Service[Command, Command]]

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")

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

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def sendToCoordinator(b: Batch, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    if(!id.equals(b.partitions.keys.toSeq.sorted.head)){
      return Future.value(true)
    }

    println(s"partition ${id} sending ack to coordinator ${b.coordinator}\n")

    val coord = coordinators(b.coordinator)
    coord(CoordinatorResult(id, conflicted, applied)).map(_ => true)
  }

  def process(b: Batch): Future[Command] = {
    if(coordinators.isEmpty) coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val txs = b.txs
    val c = coordinators(b.coordinator)

    Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
      val conflicted = reads.filter(_._2 == false).map(_._1)
      val applied = reads.filter(_._2 == true).map(_._1)

      write(applied).flatMap { ok =>
        c(CoordinatorResult(id, conflicted.map(_.id), applied.map(_.id)))
      }.map { _ =>
        Ack()
      }
    }

  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
    }
  }
}
