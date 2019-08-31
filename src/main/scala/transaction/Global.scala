package transaction

import java.net.InetSocketAddress
import java.util.UUID

import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object Global {

  def main(args: Array[String]): Unit = {

    val scheduler = new Scheduler(UUID.randomUUID.toString)
    val s = TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 4000), scheduler)

    Await.ready(s)
  }

}
