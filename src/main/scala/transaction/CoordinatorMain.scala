package transaction

import java.net.InetSocketAddress

import com.twitter.util.{Await, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object CoordinatorMain {

  val port = 3000
  val n = 3
  var coordinators = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    coordinators = coordinators + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    val adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", false)

    val p = Promise[Boolean]()

    adminUtils.deleteTopic("log", r => {
      println(s"topic log deleted ${r.succeeded()}\n")
      adminUtils.createTopic("log", n, 1, r => {
        println(s"topic log created ${r.succeeded()}\n")
        p.setValue(true)
      })
    })

    Await.ready(p)
    adminUtils.close(_)

    Await.all(coordinators.map { case (id, (host, port)) =>
      TransactorServer.Server().serve(new InetSocketAddress(host, port), new Coordinator(id, host, port))
    }.toSeq: _*)

  }

}
