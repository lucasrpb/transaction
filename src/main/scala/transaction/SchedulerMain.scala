package transaction

import java.net.InetSocketAddress

import com.twitter.util.Await
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.admin.AdminUtils

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object SchedulerMain {
  def main(args: Array[String]): Unit = {

    val adminUtils = AdminUtils.create(Vertx.vertx(), "localhost:2181", true)

    val task = adminUtils.deleteTopicFuture("log").flatMap { r =>
      adminUtils.createTopicFuture("log", 1, 1)
    }

    println(s"admin task ${Await.result(task)}\n")

    Await.ready(TransactorServer.Server().serve(new InetSocketAddress("127.0.0.1", 4000), new Scheduler()))
  }
}
