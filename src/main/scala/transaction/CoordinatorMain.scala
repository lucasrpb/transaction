package transaction

import java.net.InetSocketAddress

import com.twitter.util.{Await, Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils

import scala.concurrent.ExecutionContext.Implicits.global

object CoordinatorMain {

  val port = 3000
  val n = 2
  var coordinators = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    coordinators = coordinators + (i.toString -> ("127.0.0.1" -> (port + i)))
  }

  def main(args: Array[String]): Unit = {

    val admin = AdminUtils.create(Vertx.vertx(), "localhost:2181", false)

    val p = Promise[Boolean]()

    admin.deleteTopic("batches", r => {
      println(s"topic batches deleted ${r.succeeded()}")

      admin.deleteTopic("log", r => {

        println(s"topic log deleted ${r.succeeded()}")

        admin.createTopic("batches", 3, 1, r => {

          println(s"topic batches created ${r.succeeded()}")

          admin.createTopic("log", 1, 1, r => {

            println(s"topic log created ${r.succeeded()}")

            p.setValue(true)
          })

        })
      })
    })

    Await.ready(p)

    Await.all(coordinators.map { case (id, (host, port)) =>
      TransactorServer.Server().serve(new InetSocketAddress(host, port), new Coordinator(id, host, port))
    }.toSeq: _*)

  }

}
