package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object PartitionMain {

  val port = 5000
  val n = PARTITIONS
  var partitions = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    partitions = partitions + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    Await.all(partitions.map { case (id, (host, port)) =>
      TransactorServer.Server().serve(new InetSocketAddress(host, port), new Partition(id))
    }.toSeq: _*)

  }
}
