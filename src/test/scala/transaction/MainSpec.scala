package transaction

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.driver.core.Cluster
import com.twitter.util.{Await, Future}
import org.scalatest.FlatSpec
import transaction.protocol._

import scala.concurrent.ExecutionContext.Implicits.global

class MainSpec extends FlatSpec {

  "amount of money" should " be equal after transactions" in {

    val cluster = Cluster.builder()
      .addContactPoint("127.0.0.1")
      .build()

    val session = cluster.connect("mvcc")
    session.execute("truncate batches;")

    val rand = ThreadLocalRandom.current()

    val nAccounts = 1000
    val numExecutors = DataPartitionMain.partitions.size

    var tasks = Seq.empty[Future[Boolean]]
    val nAcc = 1000

    var clients = Seq.empty[Client]

    val counter = new AtomicInteger(0)

    for(i<-0 until 1000){

      val tid = UUID.randomUUID.toString
      val k1 = rand.nextInt(0, nAcc).toString
      val k2 = rand.nextInt(0, nAcc).toString

      if(!k1.equals(k2)){

        val c = new Client(i.toString, numExecutors)
        clients = clients :+ c

        tasks = tasks :+ c.execute(tid, Seq(k1, k2)){ case (tid, reads) =>

          val keys = reads.keys

          val k1 = keys.head
          val k2 = keys.last

          var b1 = reads(k1).v
          var b2 = reads(k2).v

          if(b1 > 0){
            val ammount = if(b1 == 1) 1 else rand.nextLong(1, b1)

            b1 = b1 - ammount
            b2 = b2 + ammount
          }

          Map(k1 -> MVCCVersion(k1, b1, tid), k2 -> MVCCVersion(k2, b2, tid))
        }
      }
    }

    val t0 = System.currentTimeMillis()
    val result = Await.result(Future.collect(tasks))
    val elapsed = System.currentTimeMillis() - t0

    println(result)

    val len = result.length
    val reqs = (1000 * len)/elapsed

    println(s"elapsed: ${elapsed}ms, req/s: ${reqs} avg. latency: ${elapsed.toDouble/len} ms\n")
    println(s"${result.count(_ == true)}/${len}\n")

    Await.all(clients.map(_.close()): _*)
  }

}
