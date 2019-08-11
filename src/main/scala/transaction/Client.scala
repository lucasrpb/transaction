package transaction

import java.util.{TimerTask, UUID}
import java.util.concurrent.{ThreadLocalRandom, TimeUnit}

import com.datastax.driver.core.Cluster
import com.twitter.finagle.Service
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.util.{Await, Duration, Future, Promise, TimeoutException, Timer}
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global

object Client {

  val rand = ThreadLocalRandom.current()

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build();

  val session = cluster.connect("mvcc")

  session.execute("truncate transactions;")

  val INSERT_TRANSACTION = session.prepare("insert into transactions(id, status, tmp) values(?,?,?);")
  val COMMIT_TRANSACTION = session.prepare("update transactions set status=? where id=? if status=? " +
    "and (tounixtimestamp(currenttimestamp()) - tmp) < ?;")
  val ABORT_TRANSACTION = session.prepare("update transactions set status=? where id=? if status=?")
  val CHECK_DATA_VERSION = session.prepare("select key from data where key=? and version=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")

  var connections = Seq.empty[(Service[Command, Command], Service[Command, Command])]

  //val connections = TrieMap[String, (Service[Command, Command], Service[Command, Command])]()

  def readData(key: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, key)).map { rs =>
      val one = rs.one()
      key -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def main(args: Array[String]): Unit = {

    val nAccounts = 1000

    def transact(tid: String, rs: Seq[String])(f: (Map[String, VersionedValue]) => Map[String, VersionedValue]): Future[Boolean] = {

      var res: ReadResult = null

      var s1: Service[Command, Command] = null
      var ex1: Service[Command, Command] = null

      if(connections.size < 256){
        s1 = createConnection("127.0.0.1", 2551)
        ex1 = createConnection("127.0.0.1", 2552)

        connections = connections :+ (s1 -> ex1)
      } else {
        val conn = connections(rand.nextInt(0, connections.size))

        s1 = conn._1
        ex1 = conn._2
      }

      val tmp = System.currentTimeMillis()

      def createTx(): Future[Boolean] = {
        session.executeAsync(INSERT_TRANSACTION.bind.setString(0, tid).setInt(1, Status.PENDING).setLong(2, tmp)).map(_.wasApplied())
      }

      def commit(): Future[Boolean] = {
        session.executeAsync(s"update transactions set status=${Status.COMMITTED} where id='${tid}' if status=${Status.PENDING};").map { r =>

          ex1(Commit(tid))

          println(s"tx ${tid} succeeded....")

          r.wasApplied()
        }
      }

      def abort(): Future[Boolean] = {
        session.executeAsync(ABORT_TRANSACTION.bind.setInt(0, Status.ABORTED).setString(1, tid).setInt(2, Status.PENDING))
          .map(_ => false)
      }

      def checkVersion(k: String, version: String): Future[Boolean] = {
        session.executeAsync(CHECK_DATA_VERSION.bind.setString(0, k).setString(1, version)).map(_.one() != null)
      }

      def checkVersions(reads: Map[String, VersionedValue]): Future[Boolean] = {

        Future.collect(reads.map{case (k, v) => checkVersion(k, v.version)}.toSeq).map { r =>
          if(r.exists(_ == false)){
            println(s"VERSIONS CHANGED...")
            false
          } else {
            true
          }
        }
      }

      def write(reads: Map[String, VersionedValue], ws: Map[String, VersionedValue]): Future[Boolean] = {
        val e = Enqueue(tid, rs, ws)

        s1(e).flatMap { r =>

          r match {
            case cmd: Ack => checkVersions(reads)
            case cmd: Nack => Future.value(false)
          }
        }
      }

      def read(): Future[Boolean] = {

        ex1(Read(rs)).flatMap { r =>
          res = r.asInstanceOf[ReadResult]

          if(res.values.isEmpty){
            //println(s"EXECUTOR IS PROCESSING...")
            Future.value(false)
          } else {
            write(res.values, f(res.values))
          }
        }
      }

      val promise = Promise[Boolean]()
      val timer = new java.util.Timer()
      timer.schedule(new TimerTask {
        override def run(): Unit = {
          promise.setException(new TimeoutException(":("))
        }
      }, TIMEOUT)

      val task = createTx().flatMap { ok =>
        if(ok) {
          read()
        } else {
          Future.value(false)
        }
      }

      val t0 = System.currentTimeMillis()
      Future.select(Seq(promise, task)).flatMap { case (r, _) =>
        if(r.isReturn && r.get()){
          commit()
        } else {
          abort()
        }
      }.rescue { case t =>

        val elapsed = System.currentTimeMillis() - t0

        println(s"tx ${tid} timed out elapsed: ${elapsed} ms...")
        abort()
      }
    }

    var tasks = Seq.empty[Future[Boolean]]
    val nAcc = 1000

    for(i<-0 until 1000){

      val tid = UUID.randomUUID.toString
      val k1 = rand.nextInt(0, nAcc).toString
      val k2 = rand.nextInt(0, nAcc).toString

      if(!k1.equals(k2)){
        tasks = tasks :+ transact(tid, Seq(k1, k2)){ reads =>

          val keys = reads.keys

          val k1 = keys.head
          val k2 = keys.last

          var b1 = reads(k1).value
          var b2 = reads(k2).value

          if(b1 > 0){
            val ammount = if(b1 == 1) 1 else rand.nextLong(1, b1)

            b1 = b1 - ammount
            b2 = b2 + ammount
          }

          Map(k1 -> VersionedValue(tid, b1), k2 -> VersionedValue(tid, b2))
        }
      }
    }

    val t0 = System.currentTimeMillis()
    val result = Await.result(Future.collect(tasks))
    val elapsed = System.currentTimeMillis() - t0

    println(result)

    val len = result.length
    val reqs = (1000 * len)/elapsed

    println(s"elapsed: ${elapsed}ms, req/s: ${reqs}\n")
    println(s"${result.count(_ == true)}/${len}\n")

    session.close()
    cluster.close()

    Await.all(connections.map{case (c1, c2) => Future.collect(Seq(c1.close(), c2.close()))}.toSeq: _*)
  }
}
