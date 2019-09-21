package transaction

import java.util.concurrent.atomic.AtomicLong
import com.datastax.driver.core.Cluster
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.concurrent.ExecutionContext

class Aggregator()(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc2")

  session.execute("truncate table log;")
  session.execute("truncate table batches;")

  val pos = new AtomicLong(0L)

  val INSERT_EPOCH = session.prepare("insert into log(offset, batch) values(?,?);")

  def insert(id: String): Future[Boolean] = {
    session.executeAsync(INSERT_EPOCH.bind.setLong(0, pos.getAndIncrement()).setString(1, id)).map { rs =>
      val ok = rs.wasApplied()
      ok
    }
  }

  def process(batch: Batch): Future[Command] = {
    insert(batch.id).map(ok => if(ok) Ack() else Nack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
    }
  }
}
