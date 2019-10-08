package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object WorkerMain {

  val port = 5000
  val n = 5
  var workers = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    workers = workers + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    Await.all(workers.map { case (id, (host, port)) =>
      TransactorServer.Server().serve(new InetSocketAddress(host, port), new Worker(id))
    }.toSeq: _*)

  }
}
