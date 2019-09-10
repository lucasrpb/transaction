package transaction

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap

class BatchServer() extends Service[Command, Command] {

  val BATCHES = TrieMap.empty[String, AtomicInteger]

  def process(cmd: Batch): Future[Command] = {
    BATCHES.put(cmd.id, new AtomicInteger(0))
    Future.value(Ack())
  }

  def process(cmd: GetBatch): Future[Command] = {
    Future.value(GetBatchResponse.apply(BATCHES(cmd.id).get()))
  }

  def process(cmd: IncBatch): Future[Command] = {
    BATCHES(cmd.id).incrementAndGet()
    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
      case cmd: GetBatch => process(cmd)
      case cmd: IncBatch => process(cmd)
    }
  }

}
