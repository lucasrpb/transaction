package transaction

import java.util.TimerTask

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Worker(val id: String)(implicit val ec: ExecutionContext) extends Service[Command, Command]{

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  var coordinators = Map.empty[String, Service[Command, Command]]
  var partitions = Map.empty[String, Service[Command, Command]]

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"partition_${id}")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  val partition = new TopicPartition(new io.vertx.kafka.client.common.TopicPartition("log", id.toInt))
  consumer.assign(partition)

  /*consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer subscribed")
    }
    case Failure(cause) => println("Failure")
  }*/

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=? and version=?;")
  val READ_OFFSET = session.prepare("select offset from offsets where id=?;")
  val UPDATE_OFFSET = session.prepare("update offsets set offset = offset + 1 where id=?;")

  def readKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k).setString(1, v.version)).map{_.one() != null}
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r)}).map(!_.contains(false))
  }

  val wb = new BatchStatement()

  def writeTx(t: Transaction): Future[Boolean] = {
    wb.clear()

    t.ws.foreach { x =>
      val k = x.k
      val v = x.v
      val version = x.version

      wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  val batch = TrieMap[String, (Transaction, Seq[String])]()
  //val processing = TrieMap[String, (Transaction, Seq[String])]()

  def handle(evts: KafkaConsumerRecords[String, Array[Byte]]): Unit = {

    if(coordinators.isEmpty) coordinators = CoordinatorMain.coordinators.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    if(partitions.isEmpty) partitions = PartitionMain.partitions.map{ case (id, (host, port)) =>
      id -> createConnection(host, port)
    }

    var keys = Seq.empty[String]

    val txs = (0 until evts.size()).map { i =>
      val r = evts.recordAt(i)
      Any.parseFrom(r.value()).unpack(Transaction)
    }

    txs.sortBy(_.id).foreach { t =>
      if(!t.keys.exists{k => keys.contains(k)}){
        keys = keys ++ t.keys
        batch.put(t.id, t -> Seq())
      }
    }

    //println(s"batch ${batch.map(_._2._1.id)}\n")

    /*coordinators(id)(CoordinatorResult(id, Seq(), txs.map(_.id))).onSuccess { _ =>
      consumer.commit()
    }*/
  }

  consumer.handler(_ => {})
  consumer.batchHandler(handle)

  val timer = new java.util.Timer()

  class Job() extends TimerTask {
    override def run(): Unit = {

      if(batch.isEmpty){
        return
      }

      val requests = TrieMap[String, PartitionRequest]()

      batch.foreach { case (tid, (t, _)) =>
        t.partitions.keys.foreach { p =>
          requests.get(p) match {
            case None => requests.put(p, PartitionRequest(p, Seq(t)))
            case Some(r) => requests.update(p, PartitionRequest(p, r.txs :+ t))
          }
        }
      }

      println(s"requests ${requests.map(_._1)}\n")

      requests.map{ case (p, r) =>
        partitions(p)(r)
      }
    }
  }

  timer.scheduleAtFixedRate(new Job(), 10L, 10L)

  def process(response: PartitionResponse): Future[Command] = {

    val c = coordinators(id)

    response.txs.foreach { t =>
      batch.get(t.id) match {
        case None =>
        case Some((t, parts)) => batch.update(t.id, t -> (parts :+ response.id))
      }
    }

    val txs = batch.filter { case (tid, (t, parts)) =>
      parts.length == t.partitions.keys.size
    }.map(_._2._1)

    val requests = TrieMap[String, PartitionRelease]()

    txs.foreach { t =>

      batch.remove(t.id)

      t.partitions.keys.foreach { p =>
        requests.get(p) match {
          case None => requests.put(p, PartitionRelease(p, Seq(t.id)))
          case Some(r) => requests.update(p, PartitionRelease(p, r.txs :+ t.id))
        }
      }
    }

    Future.collect(txs.map{t => checkTx(t).map(t -> _)}.toSeq).flatMap { reads =>

      val conflicted = reads.filter(_._2 == false).map(_._1)
      val applied = reads.filter(_._2 == true).map(_._1)

      write(applied).flatMap { ok =>
        c(CoordinatorResult(id, conflicted.map(_.id), applied.map(_.id)))
      }.flatMap{ _ =>
        Future.collect(requests.map{case (p, cmd) => partitions(p)(cmd)}.toSeq)
      }.map { _ =>

        if(batch.isEmpty){
          consumer.commit()
        }

        Ack()
      }
    }
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: PartitionResponse => process(cmd)
    }
  }
}
