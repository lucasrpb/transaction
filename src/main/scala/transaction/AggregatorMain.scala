package transaction

import java.net.InetSocketAddress
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object AggregatorMain {

  def main(args: Array[String]): Unit = {
    Await.ready(TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 4000),
      new Aggregator()))
  }

}
