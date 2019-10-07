package transaction

import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Partition(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val txs = TrieMap[String, Transaction]()

  def process(request: PartitionRequest): Future[Command] = synchronized {
    val keys = txs.values.map(_.keys).flatten.toSeq
    var granted = request.txs.filter{t => !t.keys.exists(keys.contains(_))}

    granted = granted.filter { t =>
      if(!txs.isDefinedAt(t.id)){
        txs.put(t.id, t)
        true
      } else {
        false
      }
    }

    println(s"granted ${granted.map(_.id)}\n")

    Future.value(PartitionResponse(id, granted))
  }

  def process(cmd: PartitionRelease): Future[Command] = synchronized {
    cmd.txs.foreach(txs.remove(_))
    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: PartitionRequest => process(cmd)
      case cmd: PartitionRelease => process(cmd)
    }
  }
}
