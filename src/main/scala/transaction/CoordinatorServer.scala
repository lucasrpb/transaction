package transaction

import java.net.InetSocketAddress
import java.util.UUID
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object CoordinatorServer {

  val coordinators = Map(
    "0" -> ("127.0.0.1" -> 2551),
    "1" -> ("127.0.0.1" -> 2553),
    "2" -> ("127.0.0.1" -> 2557),
    "3" -> ("127.0.0.1" -> 2559)
  )

  val n = coordinators.size

  def main(args: Array[String]): Unit = {

    Await.all(coordinators.map { case (id, (host, port)) =>
      val executor = new Coordinator(id, host, port)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)
  }

}
