package transaction

import java.net.InetSocketAddress

import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object SingletonMain {

  val port = 4000

  def main(args: Array[String]): Unit = {
    val aggregator = new Aggregator()

    Await.ready(TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", port), new Scheduler()))
  }

}
