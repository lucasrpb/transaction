package transaction

import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.Cluster
import com.twitter.finagle.Service
import com.twitter.util.Future

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Resolver(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val p = id.toInt

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")
  val partitions = TrieMap[String, Seq[String]]()

  override def apply(request: Command): Future[Command] = {
    null
  }
}
