package transaction

import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Partition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{



  override def apply(request: Command): Future[Command] = {
    null
  }
}
