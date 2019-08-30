package transaction

import java.net.InetSocketAddress
import java.util.UUID

import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object DataPartitionServer {

  val port = 2000
  val n = 3
  var partitions = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    partitions = partitions + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    val processor = new Scheduler(UUID.randomUUID.toString)
    val s = TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 4000), processor)

    Await.all(partitions.map { case (id, (host, port)) =>
      val executor = new DataPartition(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq :+ s: _*)

  }

}
