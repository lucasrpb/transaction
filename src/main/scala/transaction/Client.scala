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
    println(s"tx ${tx.id} bytes ${bytes.length}")
    session.executeAsync(INSERT_TRANSACTION.bind.setString(0, tx.id).setInt(1, Status.PENDING).setLong(2, tmp)
      .setBytes(3, ByteBuffer.wrap(bytes))).map { rs =>

      println("inserted", rs.wasApplied())

      rs.wasApplied()
    }.handle { case t =>

        t.printStackTrace()

        false
    }
  }

  def execute(tid: String, keys: Seq[String])(f: (Map[String, VersionedValue]) => Map[String, VersionedValue]): Future[Boolean] = {

    Future.collect(keys.map{k => read(k)}).flatMap { reads =>

      println(s"read ${reads}")

      val writes = f(reads.toMap)

      val tx = Transaction(tid)

      val requests = TrieMap[String, Enqueue]()

      reads.foreach { case (k, v) =>
        val p = (k.toInt % numExecutors).toString

        requests.get(p) match {
          case None => requests.put(p, Enqueue(id, Map(k -> v)))
          case Some(e) => e.addRs(k -> v)
        }
      }

      writes.foreach { case (k, v) =>
        val p = (k.toInt % numExecutors).toString

          requests.get(p) match {
            case None => requests.put(p, Enqueue(id, reads.toMap, Map(k -> v)))
            case Some(e) => e.addWs(k -> v)
          }
      }

      println(s"writes ${writes}")

      tx.addAllPartitions(requests)

      println(s"requests ${requests}")
      println(s"sending tx ${tx}")

      insertTx(tx).flatMap { ok =>

        if(!ok){
          Future.value(false)
        } else {
          val c = coordinators(if(coordinators.size == 1) "0" else rand.nextInt(0, coordinators.size).toString)

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
