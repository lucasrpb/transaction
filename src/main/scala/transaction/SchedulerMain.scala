package transaction

import java.net.InetSocketAddress

import com.twitter.util.{Await, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object SchedulerMain {
  def main(args: Array[String]): Unit = {

    val adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true)

    val p = Promise[Boolean]()

    // Delete topic 'myNewTopic'
    adminUtils.deleteTopic("log", (r) => {
      println(s"topic log deleted ${r.succeeded()}\n")
      adminUtils.createTopic("log", 1, 1, (r) => {
        println(s"topic log created ${r.succeeded()}\n")
        p.setValue(true)
      })
    })

    Await.result(p)

    Await.ready(TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 4000), new Scheduler()))
  }
}
