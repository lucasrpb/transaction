package transaction

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Timer}
import com.twitter.conversions.DurationOps._
import transaction.protocol._
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Client(val id: String, val numExecutors: Int)(implicit val ec: ExecutionContext) {

  val rand = ThreadLocalRandom.current()

  val coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  implicit val timer = new com.twitter.util.JavaTimer()

  def execute(tid: String, keys: Seq[String])(f: ((String, Map[String, MVCCVersion])) => Map[String, MVCCVersion]): Future[Boolean] = {

    val conn = coordinators(rand.nextInt(0, coordinators.size).toString)

    conn(ReadRequest(keys)).flatMap { r =>
      val reads = r.asInstanceOf[ReadResponse].values
      val writes = f(tid -> reads.map(v => v.k -> v).toMap)

      val tx = Transaction(tid, reads, writes.map(_._2).toSeq)

      conn(tx).map {
        _ match {
          case cmd: Ack =>
            println(s"tx ${tid} succeed")
            true
          case cmd: Nack =>
            println(s"tx ${tid} failed")
            false
        }
      }
    }
  }

  def close(): Future[Boolean] = {
    Future.collect(coordinators.map(_._2.close()).toSeq).map(_ => true)
  }

}
