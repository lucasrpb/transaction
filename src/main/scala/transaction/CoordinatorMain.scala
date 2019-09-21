package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object CoordinatorMain {

  val port = 3000
  val n = 3
  var coordinators = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    coordinators = coordinators + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    Await.all(coordinators.map { case (id, (host, port)) =>
      TransactorServer.Server().serve(new InetSocketAddress(host, port), new Coordinator(id, host, port))
    }.toSeq: _*)

  }

}
