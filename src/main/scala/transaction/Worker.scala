package transaction

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.concurrent.ExecutionContext

class Worker(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  /*lazy val scheduler: Service[Command, Command] = {
    createConnection("127.0.0.1", SingletonMain.port)
  }*/

  lazy val coordinators: Map[String, Service[Command, Command]] = CoordinatorMain.coordinators.map { case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

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

  val wb = new BatchStatement()

  def write(t: Transaction): Future[Boolean] = {
    wb.clear()

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

  def process(cmd: BatchInfo): Future[Command] = {
    readBatch(cmd.id).flatMap { b =>

      Future.collect(b.txs.map{t => checkTx(t).map(t -> _)}).flatMap { reads =>
        val conflicted = reads.filter(_._2 == false).map(_._1)
        val applied = reads.filter(_._2 == true).map(_._1)

        write(applied).flatMap { ok =>
          val c = coordinators(cmd.coordinator)
          c(CoordinatorResult(id, conflicted.map(_.id), applied.map(_.id)))
        }.map { _ =>
          Ack()
        }
      }
    }
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: BatchInfo => process(cmd)
    }
  }
}
