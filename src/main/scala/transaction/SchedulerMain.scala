package transaction

import scala.concurrent.ExecutionContext.Implicits.global

object SchedulerMain {

  def main(args: Array[String]): Unit = {
    val scheduler = new Scheduler()
  }

}
