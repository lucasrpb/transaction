package transaction

import scala.concurrent.ExecutionContext.Implicits.global

object AggregatorMain {
  def main(args: Array[String]): Unit = {
    new Aggregator()
  }
}
