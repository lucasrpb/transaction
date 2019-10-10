package transaction
import scala.concurrent.ExecutionContext.Implicits.global

object WorkerMain {

  /*val port = 4000
  val n = CoordinatorMain.n
  var workers = Map.empty[String, (String, Int)]

  for(i<-0 until n){
    workers = workers + (i.toString -> ("127.0.0.1" -> (port + i)))
  }*/

  def main(args: Array[String]): Unit = {

    for(i<-0 until CoordinatorMain.n){
      val w = new Worker(i.toString)
    }

  }

}
