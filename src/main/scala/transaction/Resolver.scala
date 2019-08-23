package transaction

import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Resolver(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val coordinators = TrieMap[String, Seq[String]]()

  val conns = TrieMap[String, Service[Command, Command]]()

  def send(id: String): Service[Command, Command] = {

    if(conns.isDefinedAt(id)) return conns(id)

    val info = CoordinatorServer.coordinators(id)
    val conn = createConnection(info._1, info._2)

    conns.put(id, conn)

    conn
  }

  def process(r: PartitionRequest): Future[Command] = {

    println(s"partition request from ${r.id} requesting partitions ${r.partitions}\n")

    if(coordinators.isDefinedAt(r.id)) {
      send(r.id)(PartitionResponse.apply(id, Seq.empty[String]))
      return Future.value(PartitionResponse.apply(id, Seq.empty[String]))
    }

    val partitions = r.partitions
    val lock = partitions.filter(p => !coordinators.exists{case (_, pts) => pts.contains(p)})

    if(lock.isEmpty) {
      send(r.id)(PartitionResponse.apply(id, Seq.empty[String]))
      return Future.value(PartitionResponse.apply(id, Seq.empty[String]))
    }

    coordinators.putIfAbsent(r.id, lock)

    //Future.value(PartitionResponse.apply(id, lock))

    send(r.id)(PartitionResponse.apply(id, lock))

    Future.value(Ack())
  }

  def process(r: PartitionRelease): Future[Command] = {

    println(s"partition release from ${r.id}\n")

    coordinators.remove(r.id)

    Future.value(Ack())
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: PartitionRequest => process(cmd)
      case cmd: PartitionRelease => process(cmd)
    }
  }
}
