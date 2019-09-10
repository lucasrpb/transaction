package transaction

import java.net.InetSocketAddress

import com.twitter.util.Await

object BatchMain  {

  def main(args: Array[String]): Unit = {
    val bs = new BatchServer()
    Await.ready(TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 5000), bs))
  }
  
}
