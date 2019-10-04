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
  val READ_DATA = session.prepare("select * from data where key=? and version=?;")

  def readKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k).setString(1, v.version)).map{_.one() != null}
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k))
      .map{_.wasApplied()}
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r)}).map(!_.contains(false))
  }

  def write(t: Transaction):  Future[Boolean] = {
    Future.collect(t.ws.map{v => writeKey(v.k, v)}).map(!_.contains(false))
  }

  val wb = new BatchStatement()

  /*def write(t: Transaction): Future[Boolean] = {
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
  }*/

  def sendToCoordinator(b: Batch, conflicted: Seq[String], applied: Seq[String]): Future[Boolean] = {
    if(!id.equals(b.partitions.keys.toSeq.sorted.head)){
      return Future.value(true)
    }

    println(s"partition ${id} sending ack to coordinator ${b.coordinator}\n")

    val coord = coordinators(b.coordinator)
    coord(CoordinatorResult(id, conflicted, applied)).map(_ => true)
  }

  def process(t: Transaction): Future[Command] = {

    if(coordinators.isEmpty) coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val c = coordinators("0")

    checkTx(t).flatMap { case ok =>

      if(!ok){
        c(CoordinatorResult(id, Seq(t.id), Seq()))
      } else {
        write(t).flatMap { ok =>

          if(!ok){
            println(s"writes = ${ok}\n")
          }

          c(CoordinatorResult(id, if(!ok) Seq(t.id) else Seq(), if(ok) Seq(t.id) else Seq()))
        }
      }
    }.map { _ =>
      Ack()
    }.handle { case ex =>
      ex.printStackTrace()
      Nack()
    }
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
    }
  }
}
