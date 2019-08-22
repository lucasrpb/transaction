package transaction

import java.util.TimerTask
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.Cluster
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
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

  val READ_TRANSACTION = session.prepare("select * from transactions where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val UPDATE_TRANSACTION = session.prepare("update transactions set status=? where id=? if status=?")

  def write(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k)).map(_.wasApplied())
  }

  val coordinators = CoordinatorServer.coordinators.map{ case (id, (host, port)) =>
    id -> (0 until 1).map(_ => createConnection(host, port))
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
    session.executeAsync(READ_DATA.bind.setString(0, k)).map{rs =>
      val one = rs.one()
      one != null && one.getLong("value") == v.value}
  }

  def writeKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.rs.map{case (k, v) => readKey(k, v)}.toSeq).map(!_.contains(false))
  }

  def writeTx(t: Transaction): Future[Boolean] = {
    Future.collect(t.ws.map{case (k, v) => writeKey(k, v)}.toSeq).map(!_.contains(false))
  }

  def commitTx(t: Transaction): Future[Boolean] = {
    session.executeAsync(UPDATE_TRANSACTION.bind.setInt(0, Status.COMMITTED).setString(1, t.id)
      .setInt(2, Status.PENDING)).map(_.wasApplied())
  }

  def abortTx(t: Transaction): Future[Boolean] = {
    session.executeAsync(UPDATE_TRANSACTION.bind.setInt(0, Status.ABORTED).setString(1, t.id)
      .setInt(2, Status.PENDING)).map(_.wasApplied())
  }

  val rand = ThreadLocalRandom.current()

  def send(c: String, r: PartitionResult): Future[Command] = {
    val connections = coordinators(c)
    connections.apply(rand.nextInt(0, connections.size))(r)
  }

  consumer.handler((evt: KafkaConsumerRecord[String, Array[Byte]]) => {

    val bytes = evt.value()
    val obj = Any.parseFrom(bytes)
    val batch = obj.unpack(Batch)

    val txs = batch.transactions

    consumer.pause()

    println(s"executor ${id} processing batch ${batch.id} with size ${txs.size} at partition ${evt.partition()}")

    Future.collect(txs.map(t => checkTx(t).map(t -> _))).flatMap { checks =>
      val conflicted = checks.filter(_._2 == false)
      val accepted = checks.filter(_._2 == true)

      // Assuming nothing goes wrong...
      Future.collect(accepted.map{case (t, _) => writeTx(t)}).flatMap { writes =>
        Future.collect(accepted.map{case (t, _) => commitTx(t)}).map { commits =>
          send(batch.coordinator, PartitionResult.apply(batch.id, conflicted.map(_._1.id), accepted.map(_._1.id)))
          consumer.resume()
        }
      }
    }.handle { case t =>
      t.printStackTrace()
    }

  })

  override def apply(request: Command): Future[Command] = {
    null
  }
}
