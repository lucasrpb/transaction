package transaction

import java.net.InetSocketAddress

import com.twitter.util.Await

import scala.concurrent.ExecutionContext.Implicits.global

object ExecutorServer {

  def main(args: Array[String]): Unit = {

    val id = "1"
    val executor = new Executor(id)

    val e1 = TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 2552)
      , executor)

    Await.ready(e1)

  }

}
