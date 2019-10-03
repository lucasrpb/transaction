package transaction

import java.util.{Timer, TimerTask}

import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Scheduler()(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  var workers = Map.empty[String, Service[Command, Command]]

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"scheduler")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  val executing = TrieMap[String, Batch]()
  //val batches = TrieMap[String, Batch]()

  def handle(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {

    consumer.pause()

    if(workers.isEmpty) workers = WorkerMain.workers.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    val batches = TrieMap[String, Batch]()

    (0 until evts.size).foreach { idx =>
      val b = Any.parseFrom(evts.recordAt(idx).value()).unpack(Batch)
      batches.put(b.id, b)
    }

    def execute(): Future[Boolean] = {

      var partitions = Seq.empty[String]

      var list = batches.filter { case (_, b) =>
        val pset = b.partitions.keys.toSeq

        if(!pset.exists{partitions.contains(_)}){
          partitions = partitions ++ pset
          true
        } else {
          false
        }
      }.values.toSeq

      println(s"not conflicting ${list.size}\n")

      val WORKERS = workers.values.toSeq
      list = list.slice(0, Math.min(WORKERS.size, list.size))

      Future.collect(list.zipWithIndex.map{case (b, idx) => WORKERS(idx)(b)}).flatMap { acks =>

        println(s"executed ${list.size}\n...")

        list.foreach { b =>
          batches.remove(b.id)
        }

        if(batches.isEmpty){
          Future.value(true)
        } else {
          execute()
        }
      }
    }

    execute().ensure {
      consumer.resume()
      consumer.commit()
    }

    /*val w = workers.head._2

    Future.traverseSequentially(batches) { b =>
      w.apply(b)
    }.handle { case ex =>
      ex.printStackTrace()
    }.ensure {
      consumer.commit()
    }*/
  }

  /*val timer = new Timer()

  class Job() extends TimerTask {
    override def run(): Unit = {



    }
  }

  timer.schedule(new Job(), 10L)*/

  consumer.handler(_ => {})
  consumer.batchHandler(handle)

  override def apply(request: Command): Future[Command] = {
    null
  }
}
