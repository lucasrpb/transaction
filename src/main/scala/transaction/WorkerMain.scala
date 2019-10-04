package transaction

import java.net.InetSocketAddress

import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object WorkerMain {

  val port = 2000
  val n = 1
  var workers = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    workers = workers + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {
    Await.all(workers.map { case (id, (host, port)) =>
      val worker = new Worker(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), worker)
    }.toSeq: _*)

  }

}
