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

  val coordinators = Map(
    "0" -> createConnection("127.0.0.1", 2551)
  )

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val rand = ThreadLocalRandom.current()

  val INSERT_TRANSACTION = session.prepare("insert into transactions(id, status, tmp, bin) values(?,?,?, ?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def read(k: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map { r =>
      val one = r.one()
      k -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def insertTx(tx: Transaction): Future[Boolean] = {
    val tmp = System.currentTimeMillis()
    val bytes = Any.pack(tx).toByteArray

    session.executeAsync(INSERT_TRANSACTION.bind.setString(0, tx.id).setInt(1, Status.PENDING).setLong(2, tmp)
      .setBytes(3, ByteBuffer.wrap(bytes))).map { rs => rs.wasApplied()
    }.handle { case t =>

        t.printStackTrace()

        false
    }
  }

  def execute(tid: String, keys: Seq[String])(f: ((String, Map[String, VersionedValue])) => Map[String, VersionedValue]): Future[Boolean] = {

    Future.collect(keys.map{k => read(k)}).flatMap { reads =>

      val writes = f(tid -> reads.toMap)

      println(s"reads ${reads.toMap}")
      println(s"writes ${writes}")
      println()

      val tx = Transaction(tid, reads.toMap, writes)

      insertTx(tx).flatMap { ok =>

        if(!ok){
          Future.value(false)
        } else {
          val c = coordinators("0")

          c(tx).map {
            _ match {
              case cmd: Ack => true
              case cmd: Nack => false
            }
          }
        }
      }
    }
  }

  def close(): Future[Boolean] = {
    session.closeAsync().flatMap(_ => cluster.closeAsync()).map(_ => true)
  }

}
