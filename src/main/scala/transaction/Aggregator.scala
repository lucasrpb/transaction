package transaction

import java.util.TimerTask
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.atomic.AtomicLong

import com.datastax.driver.core.Cluster
import com.twitter.finagle.Service
import com.twitter.util.Future
import transaction.protocol._
import scala.collection.JavaConverters._

import scala.concurrent.ExecutionContext

class Aggregator()(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val timer = new java.util.Timer()
  val batch = new ConcurrentLinkedDeque[String]()

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc2")

  session.execute("truncate table log;")
  session.execute("truncate table batches;")

  val pos = new AtomicLong(0L)

  val INSERT_EPOCH = session.prepare("insert into log(offset, batches) values(?,?);")

  def insert(batches: Seq[String]): Future[Boolean] = {
    session.executeAsync(INSERT_EPOCH.bind.setLong(0, pos.getAndIncrement()).setSet(1, batches.toSet.asJava)).map { rs =>
      val ok = rs.wasApplied()
      ok
    }
  }

  def process(b: Batch): Future[Command] = {
    batch.add(b.id)
    println(s"adding batch ${b.id}\n")
    Future.value(Ack())
  }

  class Job extends TimerTask {
    override def run(): Unit = {
      var batches = Seq.empty[String]

      while(!batch.isEmpty){
        batches = batches :+ batch.poll()
      }

      if(batches.isEmpty){
        timer.schedule(new Job(), 10L)
        return
      }

      println(s"inserting epoch ${batches}\n")

      insert(batches).onSuccess { ok =>

        println(s"ok ${ok}")

        timer.schedule(new Job(), 10L)
      }.handle { case ex =>
        ex.printStackTrace()
      }
    }
  }

  timer.schedule(new Job(), 10L)

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Batch => process(cmd)
    }
  }
}
