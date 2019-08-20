package transaction

import java.net.InetSocketAddress
import java.util.UUID
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object CoordinatorServer {

  def main(args: Array[String]): Unit = {

    val serializers = Map(
      "0" -> ("127.0.0.1" -> 2551)
    )

    Await.all(serializers.map { case (id, (host, port)) =>
      val executor = new Coordinator(id, host, port)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)

  }

}
