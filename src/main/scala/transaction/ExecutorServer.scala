package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object ExecutorServer {

  val port = 2000
  val n = 10
  var executors = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    executors = executors + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {
    Await.all(executors.map { case (id, (host, port)) =>
      val executor = new Executor(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)
  }

}
