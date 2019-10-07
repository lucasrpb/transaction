package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object PartitionMain {

  val port = 4000
  val n = 10
  var partitions = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    partitions = partitions + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {
    Await.all(partitions.map { case (id, (host, port)) =>
      val p = new Partition(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), p)
    }.toSeq: _*)
  }

}
