package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object ExecutorServer {

  val executors = Map(
    "0" -> ("127.0.0.1" -> 2551),
    "1" -> ("127.0.0.1" -> 2552),
    "2" -> ("127.0.0.1" -> 2553)
  )

  val n = executors.size

  def main(args: Array[String]): Unit = {
    Await.all(executors.map { case (id, (host, port)) =>
      val executor = new Executor(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)
  }

}
