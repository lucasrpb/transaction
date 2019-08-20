package transaction

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.Session
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class Client(val id: String,
             val session: Session,
             val coordinators: Map[String, Service[Command, Command]],
             val npartitions: Int)
            (implicit val ec: ExecutionContext) {

  val rand = ThreadLocalRandom.current()

  val INSERT_TRANSACTION = session.prepare("insert into transactions(id, status, tmp, bin) values(?,?,?,?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def read(k: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind()).map { r =>
      val one = r.one()
      k -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def insertTx(tx: Transaction): Future[Boolean] = {
    val tmp = System.currentTimeMillis()
    val bytes = Any.pack(tx).toByteArray
    session.executeAsync(INSERT_TRANSACTION.bind.setString(0, id).setInt(1, Status.PENDING).setLong(2, tmp)
      .setBytes(3, ByteBuffer.wrap(bytes).flip())).map(_.wasApplied())
  }

  def execute(keys: Seq[String])(f: (Map[String, VersionedValue]) => Map[String, VersionedValue]): Future[Boolean] = {
    Future.collect(keys.map{k => read(k)}).flatMap { reads =>
      val writes = f(reads)

      val tx = Transaction(id)

      val requests = TrieMap[String, Enqueue]()

      reads.foreach { case (k, v) =>
        val p = (k.toInt % npartitions).toString

        requests.get(p) match {
          case None => requests.put(p, Enqueue(id, Map(k -> v)))
          case Some(e) => e.addRs(k -> v)
        }
      }

      writes.foreach { case (k, v) =>
        val p = (k.toInt % npartitions).toString

          requests.get(p) match {
            case None => requests.put(p, Enqueue(id, reads.toMap, Map(k -> v)))
            case Some(e) => e.addWs(k -> v)
          }
      }

      tx.addAllPartitions(requests)

      val c = coordinators(rand.nextInt(0, coordinators.size).toString)

      c(tx).map {
        _ match {
          case cmd: Ack => true
          case cmd: Nack => false
        }
      }
    }
  }

}
