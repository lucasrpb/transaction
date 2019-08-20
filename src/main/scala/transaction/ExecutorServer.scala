package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await

object ExecutorServer {

  def main(args: Array[String]): Unit = {

    val executors = Map(
      "0" -> ("127.0.0.1" -> 2552)
    )

    val coordinators = Map(
      "0" -> createConnection("127.0.0.1", 2553)
    )

    Await.all(executors.map { case (id, (host, port)) =>
      val executor = new Executor(id, coordinators)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)

  }

}
