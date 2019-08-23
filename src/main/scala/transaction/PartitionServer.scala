package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object PartitionServer {

  val resolvers = Map(
    "0" -> ("127.0.0.1" -> 2551)
  )

  val n = resolvers.size

  def main(args: Array[String]): Unit = {
    Await.all(resolvers.map { case (id, (host, port)) =>
      val executor = new Partition(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)
  }

}
