package transaction

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Client(val id: String, val numExecutors: Int)(implicit val ec: ExecutionContext) {

  val rand = ThreadLocalRandom.current()

  val coordinators = CoordinatorServer.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  def execute(tid: String, keys: Seq[String])(f: ((String, Map[String, VersionedValue])) => Map[String, VersionedValue]): Future[Boolean] = {

    val conn = coordinators(rand.nextInt(0, coordinators.size).toString)

    conn(Read(keys)).flatMap { r =>
      val reads = r.asInstanceOf[ReadResult].values
      val writes = f(tid -> reads)

      val tx = Transaction(tid, reads, writes)

      conn(tx).map {
        _ match {
          case cmd: Ack => true
          case cmd: Nack => false
        }
      }
    }
  }

  def close(): Future[Boolean] = {
    Future.collect(coordinators.map(_._2.close()).toSeq).map(_ => true)
  }

}
