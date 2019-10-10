package transaction

import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Worker(val id: String)(implicit val ec: ExecutionContext) {

  val eid = id.toInt

  lazy val coordinators: Map[String, Service[Command, Command]] = CoordinatorMain.coordinators.map { case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  val vertx = Vertx.vertx()

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> s"worker_${id}")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  /*val partition = new io.vertx.kafka.client.common.TopicPartition()
    partition.setTopic("batches")
    .setPartition(eid)

  consumer.assign(TopicPartition(partition))*/

  consumer.subscribeFuture("batches").onComplete {
    case Success(result) => {
      println(s"worker ${id} subscribed!")
    }
    case Failure(cause) => cause.printStackTrace()
  }

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mv2pl")

  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=? and version=?;")

  val ACQUIRE_PARTITION = session.prepare("update partitions set batch=? where id=? if batch=null;")
  val RELEASE_PARTITION = session.prepare("update partitions set batch=null where id=? if batch=?;")

  def acquirePartition(id: String, bid: String): Future[Boolean] = {
    session.executeAsync(ACQUIRE_PARTITION.bind.setString(0, bid).setString(1, id)).map(_.wasApplied())
  }

  def acquire(b: Batch): Future[Seq[(String, Boolean)]] = {
    Future.collect(b.partitions.map{p => acquirePartition(p, b.id).map(p -> _)})
  }

  def releasePartition(id: String, bid: String): Future[Boolean] = {
    session.executeAsync(RELEASE_PARTITION.bind.setString(0, id).setString(1, bid)).map(_.wasApplied())
  }

  def release(bid: String, partitions: Seq[String]): Future[Boolean] = {
    Future.collect(partitions.map{releasePartition(_, bid)}).map(_ => true)
  }

  def readKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k).setString(1, v.version)).map{_.one() != null}
  }

  def writeKey(k: String, v: MVCCVersion): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.v).setString(1, v.version).setString(2, k)).map{_.wasApplied()}
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{r => readKey(r.k, r)}).map(!_.contains(false))
  }

  def write(t: Transaction): Future[Boolean] = {
    val wb = new BatchStatement()

    t.ws.foreach { x =>
      val k = x.k
      val v = x.v
      val version = x.version

      wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
    }

    session.executeAsync(wb).map(_.wasApplied()).handle { case e =>
      e.printStackTrace()
      false
    }
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    val wb = new BatchStatement()

    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { x =>
        val k = x.k
        val v = x.v
        val version = x.version

        wb.add(UPDATE_DATA.bind.setLong(0, v).setString(1, version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied()).handle { case e =>
      e.printStackTrace()
      false
    }
  }

  def check(txs: Seq[Transaction]): Future[Seq[(Transaction, Boolean)]] = {
    Future.collect(txs.map{t => checkTx(t).map(t -> _)})
  }

  def execute(b: Batch, locks: Seq[String]): Future[Boolean] = {

    var conflicted = Seq.empty[String]

    val txs = b.txs.filter{ t =>
      if(t.partitions.forall(locks.contains(_))){
        true
      } else {
        conflicted = conflicted :+ t.id
        false
      }
    }

    if(txs.isEmpty){
      coordinators(b.coordinator)(CoordinatorResult(id, b.txs.map(_.id)))
      return release(b.id, locks)
    }

    check(txs).flatMap { reads =>
      conflicted = conflicted ++ reads.filter(_._2 == false).map(_._1.id)
      val applied = reads.filter(_._2 == true).map(_._1)

      write(applied).flatMap { ok =>
        release(b.id, locks).map(_ && ok).map { ok =>
          coordinators(b.coordinator)(CoordinatorResult(id, conflicted, applied.map(_.id)))
          true
        }
      }
    }

  }

  def execute(b: Batch): Future[Boolean] = {

    println(s"worker ${id} executing batch ${b.id}...\n")

    acquire(b).flatMap { locks =>
      execute(b, locks.filter(_._2).map(_._1))
    }
  }

  var batch: Option[Batch] = None

  def handler(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {
    if(evt.partition() != eid) {
      consumer.commit()
      return
    }

    val b = Any.parseFrom(evt.value()).unpack(Batch)

    println(s"processing batch ${b.id}...\n")

    consumer.pause()
    batch = Some(b)

    execute(batch.get).handle { case ex =>
      ex.printStackTrace()
    }.ensure {
      batch = None
      consumer.commit()
      consumer.resume()
    }

  }

  consumer.handler(handler)
}
