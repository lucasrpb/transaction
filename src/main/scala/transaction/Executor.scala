package transaction

import com.datastax.driver.core.{Cluster, ResultSet}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.Future
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.common.TopicPartition
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

class Executor(val id: String) extends Service[Command, Command]{

  val committed = TrieMap[String, String]()
  val executing = TrieMap[String, Enqueue]()

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build();

  val session = cluster.connect("mvcc")

  val READ_TRANSACTION = session.prepare("select * from transactions where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def readData(key: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, key)).map { rs =>
      val one = rs.one()
      key -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def read(id: String): Future[ResultSet] = {
    session.executeAsync(READ_TRANSACTION.bind.setString(0, id))
  }

  def write(key: String, value: VersionedValue): Future[ResultSet] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, value.value).setString(1, value.version).setString(2, key))
  }

  def checkTransactionStatus(b: Batch): Future[Boolean] = {
    val txs = b.txs.map(_.id)
    val txMap = b.txs.map{case t => t.id -> t}.toMap

    println(s"processing batch with size ${txs.length}...\n")

    Future.collect(txs.filter(k => !committed.contains(k)).map(read(_))).flatMap { reads =>

      reads.foreach { rs =>
        val one = rs.one()

        val id = one.getString("id")
        val status = one.getInt("status")
        val elapsed = System.currentTimeMillis() - one.getLong("tmp")

        val ok = status == Status.COMMITTED || status == Status.ABORTED || elapsed >= TIMEOUT

        if(ok) {
          committed.putIfAbsent(id, id)
        }

        if(status == Status.COMMITTED){
          executing.putIfAbsent(id, txMap(id))
        }
      }

      if(committed.size == txs.length){
        val kvs = executing.map(_._2.ws).flatten.toMap

        Future.collect(kvs.map{case (k, v) => write(k, v)}.toSeq).map { writes =>
          executing.clear()
          committed.clear()
          true
        }
      } else {
        Future.value(false)
      }
    }.handle {case t =>

      t.printStackTrace()

      false
    }
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

  consumer.subscribeFuture("transactions").onComplete{
    case Success(result) => {
      println("Consumer subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  consumer.handler((evt: KafkaConsumerRecord[String, Array[Byte]]) => {

    val bytes = evt.value()
    val p = Any.parseFrom(bytes)
    val b = p.unpack(Batch)

    checkTransactionStatus(b).map { ok =>
      if(ok) consumer.commit() else consumer.seek(TopicPartition().setPartition(evt.partition()), evt.offset())
    }
  })

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Read =>

        val kvs = executing.map(_._2.ws.map(_._1)).flatten.toSeq

        if(cmd.keys.exists(k1 => kvs.exists{k => k.equals(k1)})){
          println(s"conflict!!!")
          Future.value(ReadResult())
        } else {
          Future.collect(cmd.keys.map(readData(_))).map { reads =>
            ReadResult(reads.toMap)
          }
        }

      case cmd: Commit =>
        committed.putIfAbsent(cmd.id, cmd.id)
        Future.value(Ack())
    }
  }
}
