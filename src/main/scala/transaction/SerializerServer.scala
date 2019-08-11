package transaction

import java.net.InetSocketAddress
import java.util.UUID
import com.twitter.util.Await
import scala.concurrent.ExecutionContext.Implicits.global

object SerializerServer {

  def main(args: Array[String]): Unit = {

    val id = "1"
    val serializer = new Serializer(id)

    val s1 = TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 2551)
      , serializer)

    Await.ready(s1)

  }

}
