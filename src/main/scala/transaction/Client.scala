package transaction

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Timer}
import com.twitter.conversions.DurationOps._
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Client()(implicit val ec: ExecutionContext) {

  val tid = UUID.randomUUID.toString()
  val rand = ThreadLocalRandom.current()

  val coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  implicit val timer = new com.twitter.util.JavaTimer()

  def execute(f: ((String, Map[String, MVCCVersion])) => Map[String, MVCCVersion]): Future[Boolean] = {

    val accs = accounts.keys.toSeq

    val k1 = accs(rand.nextInt(0, accs.length)).toString
    val k2 = accs(rand.nextInt(0, accs.length)).toString

   // if(k1.equals(k2)) return Future.value(true)

    val keys = Seq(k1, k2)

    val conn = coordinators(rand.nextInt(0, coordinators.size).toString)

    conn(ReadRequest(keys)).flatMap { r =>
      val reads = r.asInstanceOf[ReadResponse].values
      val writes = f(tid -> reads.map(v => v.k -> v).toMap)

      var partitions = Map[String, KeyList]()

      keys.distinct.foreach { k =>
        val p = (accounts.computeHash(k).abs % PartitionMain.n).toString

        partitions.get(p) match {
          case None => partitions = partitions + (p -> KeyList(Seq(k)))
          case Some(klist) => partitions = partitions + (p -> KeyList(klist.keys :+ k))
        }
      }

      val tx = Transaction(tid, reads, writes.map(_._2).toSeq, partitions, keys.distinct)

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
