package transaction

import java.net.InetSocketAddress
import java.util.UUID

import com.twitter.util.{Await, Future}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils

import scala.concurrent.ExecutionContext.Implicits.global

object DataPartitionMain {

  val port = 2000
  val n = 10
  var partitions = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    partitions = partitions + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

   // val scheduler = new Scheduler()

    Await.all(partitions.map { case (id, (host, port)) =>
      val executor = new DataPartition(id)
      TransactorServer.Server().serve(new InetSocketAddress(host, port), executor)
    }.toSeq: _*)

  }

}
