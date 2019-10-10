package transaction

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import com.twitter.util.Future
import transaction.protocol._

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

    val keys = Seq(k1, k2)

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
