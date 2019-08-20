package transaction

import java.util.TimerTask

import com.datastax.driver.core.Cluster
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Executor(val id: String)(implicit val ec: ExecutionContext)
  extends Service [Command, Command]{

  val p = id.toInt

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val coordinators = Map(
    "0" -> createConnection("127.0.0.1", 2551)
  )

  val READ_TRANSACTION = session.prepare("select * from transactions where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=? and version=?;")
  val UPDATE_TRANSACTION = session.prepare("update transactions set status=? where id=? if status=?")

  def write(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k)).map(_.wasApplied())
  }

  val config = scala.collection.mutable.Map[String, String]()

  config += (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092")
  config += (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer")
  config += (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer")
  config += (ConsumerConfig.GROUP_ID_CONFIG -> "my_group")
  config += (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest")
  config += (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false")

  val vertx = Vertx.vertx()

  // use consumer for interacting with Apache Kafka
  var consumer = KafkaConsumer.create[String, Array[Byte]](vertx, config)

  consumer.subscribeFuture("transactions").onComplete {
    case Success(result) => {
      println(s"Consumer ${id} subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  def getTx(t: String): Future[Transaction] = {
    session.executeAsync(READ_TRANSACTION.bind.setString(0, t)).map { r =>
      val one = r.one()
      val obj = Any.parseFrom(one.getBytes("bin").array())
      obj.unpack(Transaction)
    }
  }

  def readKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k).setString(1, v.version)).map(_.one() != null)
  }

  def writeKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    val rs = t.partitions.values.map(_.rs).flatten
    Future.collect(rs.map{case (k, v) => readKey(k, v)}.toSeq).map(!_.contains(false))
  }

  def commitTx(t: Transaction): Future[Boolean] = {
    session.executeAsync(UPDATE_TRANSACTION.bind.setInt(0, Status.COMMITTED).setString(1, t.id)
      .setInt(2, Status.PENDING)).map(_.wasApplied())
  }

  def abortTx(t: Transaction): Future[Boolean] = {
    session.executeAsync(UPDATE_TRANSACTION.bind.setInt(0, Status.ABORTED).setString(1, t.id)
      .setInt(2, Status.PENDING)).map(_.wasApplied())
  }

  def applyWrites(txs: Seq[(Transaction, Boolean)], cid: String, tp: TopicPartition, offset: Long): Future[Boolean] = {
    val result = PartitionResult()
    val c = coordinators(cid)

    val conflicted = txs.filter(!_._2).map(_._1)
    result.addAllConflicted(conflicted.map(_.id))

    val applies = txs.filter{case (t, ok) => ok && t.partitions.isDefinedAt(id) && !t.partitions(id).ws.isEmpty}.map(_._1)
    val writes = applies.map(_.partitions(id).ws).flatten

    Future.collect(writes.map{case (k, v) => writeKey(k, v)}).flatMap { writes =>

      val p = Promise[Boolean]()

      if(writes.contains(false)){
        consumer.seekFuture(tp, offset).onComplete {
          case Success(r) => p.setValue(false)
          case Failure(ex) => p.setException(ex)
        }
        p
      } else {
        c(result)

        Future.collect(applies.map{t => commitTx(t).map{t -> _}} ++ conflicted.map{t => abortTx(t).map(t -> _)})
          .flatMap { _ =>

            result.addAllApplied(applies.map(_.id))

            consumer.commitFuture().onComplete {
              case Success(r) => p.setValue(true)
              case Failure(ex) => p.setException(ex)
            }

            p
          }
      }
    }
  }

  consumer.handler((evt: KafkaConsumerRecord[String, Array[Byte]]) => {

    //println(s"processing partition ${evt.partition()}...\n")

    val bytes = evt.value()
    val obj = Any.parseFrom(bytes)
    val batch = obj.unpack(Batch)

    if(!batch.transactions.isEmpty){
      println(s"processing ${batch.transactions}...\n")
    }

    Future.collect(batch.transactions.map{t => getTx(t)}).flatMap { txs =>

      println(s"transactions ${txs}\n")

      val reads = txs.filter(_.partitions.isDefinedAt(id))

      Future.collect(reads.map{t => checkTx(t).map(t -> _)}).flatMap { txs =>
        applyWrites(txs, batch.coordinator, TopicPartition().setPartition(evt.partition()), evt.offset())
      }
    }.handle { case t =>
      t.printStackTrace()
    }
  })

  override def apply(request: Command): Future[Command] = {
    null
  }
}
