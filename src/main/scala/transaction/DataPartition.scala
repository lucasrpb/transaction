package transaction

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Timer, TimerTask}

import com.datastax.driver.core.{BatchStatement, Cluster}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import io.vertx.core.Handler
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.consumer.{KafkaConsumer, KafkaConsumerRecord, KafkaConsumerRecords}
import org.apache.kafka.clients.consumer.ConsumerConfig
import transaction.protocol._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class DataPartition(val id: String)(implicit val ec: ExecutionContext) extends Service [Command, Command]{

  val eid = id.toInt

  val coordinators = CoordinatorServer.coordinators.map{ case (id, (host, port)) =>
    id -> createConnection(host, port)
  }

  //val partitions = (0 until NPARTITIONS).filter(_ % ExecutorServer.n == eid).map(_.toString)

  //println(s"executor $id partitions ${partitions}\n")

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  val READ_BATCH = session.prepare("select * from batches where id=?;")
 // val UPDATE_BATCH = session.prepare("update batches set n = n + ? where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")

  val wb = new BatchStatement()

  /*def readBatch(bid: String, total: Int): Future[Boolean] = {
    session.executeAsync(READ_BATCH.bind.setString(0, bid)).map { rs =>
      val n = rs.one.getInt("n")
      println(s"batch total at executor ${id} batch ${bid} ${n}/${total}\n")
      total == n
    }
  }*/

  def readBatch(bid: String): Future[Batch] = {
    session.executeAsync(READ_BATCH.bind.setString(0, bid)).map { rs =>
      Any.parseFrom(rs.one().getBytes("bin").array()).unpack(Batch)
    }
  }

  def updateBatch(bid: String, total: Int, n: Int): Future[Boolean] = {
    //session.executeAsync(UPDATE_BATCH.bind.setInt(0, n).setString(1, id)).map(_.one.getInt("n") == total)
    val q = s"update batches set n = n + ${n} where id='${bid}';"
    session.executeAsync(q).map {_.wasApplied()}
  }

  def readKey(k: String, v: VersionedValue, tx: String): Future[Boolean] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map{rs =>
      val one = rs.one()
      one != null && one.getString("version").equals(v.version) || one.getString("version").equals(tx)}
  }

  def writeKey(k: String, v: VersionedValue): Future[Boolean] = {
    session.executeAsync(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
      .map(_.wasApplied())
  }

  def checkTx(t: Transaction, txs: Seq[Transaction]): Future[Boolean] = {
    Future.collect(t.rs.map{case (k, v) => readKey(k, v, txs.find(_.ws.isDefinedAt(k)).get.id)}.toSeq)
      .map(!_.contains(false))
  }

  def writeTx(t: Transaction): Future[Boolean] = {
    wb.clear()

    t.ws.foreach { case (k, v) =>
      if((k.toInt % DataPartitionServer.n) == eid){
        wb.add(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

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

  consumer.subscribeFuture("log").onComplete {
    case Success(result) => {
      println(s"Consumer ${id} subscribed")
    }
    case Failure(cause) => println("Failure")
  }

  def write(txs: Seq[Transaction]): Future[Boolean] = {
    wb.clear()

    txs.foreach { t =>
      t.ws.foreach { case (k, v) =>
        if((k.toInt % NPARTITIONS) % DataPartitionServer.n == eid){
          wb.add(UPDATE_DATA.bind.setLong(0, v.value).setString(1, v.version).setString(2, k))
        }
      }
    }

    session.executeAsync(wb).map(_.wasApplied())
  }

  var offset: Long = 0L

  var others = Map.empty[String, Service[Command, Command]]

  def checkOffset(): Future[Boolean] = {
    if(others.isEmpty) {
      others = DataPartitionServer.partitions.filterNot(_._1.equals(id)).map { case (id, (host, port)) =>
        id -> createConnection(host, port)
      }
    }

    Future.collect(others.map{_._2.apply(GetOffset())}.toSeq).map { offsets =>
      offsets.map(_.asInstanceOf[OffsetResult].offset == offset).count(_ == true) == DataPartitionServer.n
    }.flatMap { ok =>
      if(ok){
        Future.value(true)
      } else {
        checkOffset()
      }
    }
  }

  /*def process(evt: KafkaConsumerRecord[String, Array[Byte]]): Unit = {

    val epoch = Any.parseFrom(evt.value()).unpack(Epoch)

    println(s"data partition ${id} processing epoch ${epoch.id}...\n")

    consumer.pause()

    offset = evt.offset()

    Future.collect(epoch.batches.map{readBatch(_)}).flatMap { batches =>

      //println(s"batches: ${batches}\n")

      var txs = batches.map(_.transactions.values).flatten
      var keys = Seq.empty[String]

      var conflicted = Seq.empty[Transaction]
      var applied = Seq.empty[Transaction]

      val coords = txs.map { t =>
        batches.find(_.transactions.isDefinedAt(t.id)).get
      }.distinct.map(b => b -> coordinators(b.coordinator))

      txs = txs.sortBy(_.id).filter { t =>
        if(!t.ws.exists{case (k, _) => keys.contains(k)}){
          keys = keys ++ t.rs.map(_._1)
          true
        } else {
          conflicted = conflicted :+ t
          false
        }
      }

      Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
        conflicted = conflicted ++ reads.filter(_._2 == false).map(_._1)
        applied = reads.filter(_._2 == true).map(_._1)

        write(applied).flatMap { ok =>
          Future.collect(coords.map { case (b, c) =>

            val CONFLICTED = conflicted.filter(t => b.transactions.isDefinedAt(t.id)).map(_.id)
            val APPLIED = applied.filter(t => b.transactions.isDefinedAt(t.id)).map(_.id)

            c(PartitionResponse(id, CONFLICTED, APPLIED))
          })
        }
      }
    }.flatMap { _ =>
      checkOffset()
    }
    .ensure {
      consumer.commit()
      consumer.resume()
    }
  }*/

  def process(epoch: Epoch): Future[Command] = {

    println(s"data partition ${id} processing epoch ${epoch.id}...\n")

    Future.collect(epoch.batches.map{readBatch(_)}).flatMap { batches =>

      //println(s"batches: ${batches}\n")

      var txs = batches.map(_.transactions.values).flatten
      var keys = Seq.empty[String]

      var conflicted = Seq.empty[Transaction]
      var applied = Seq.empty[Transaction]

      val coords = txs.map { t =>
        batches.find(_.transactions.isDefinedAt(t.id)).get
      }.distinct.map(b => b -> coordinators(b.coordinator))

      txs = txs.sortBy(_.id).filter { t =>
        if(!t.ws.exists{case (k, _) => keys.contains(k)}){
          keys = keys ++ t.rs.map(_._1)
          true
        } else {
          conflicted = conflicted :+ t
          false
        }
      }

      Future.collect(txs.map{t => checkTx(t, txs).map(t -> _)}).flatMap { reads =>
        conflicted = conflicted ++ reads.filter(_._2 == false).map(_._1)
        applied = reads.filter(_._2 == true).map(_._1)

        write(applied).flatMap { ok =>
          Future.collect(coords.map { case (b, c) =>

            val CONFLICTED = conflicted.filter(t => b.transactions.isDefinedAt(t.id)).map(_.id)
            val APPLIED = applied.filter(t => b.transactions.isDefinedAt(t.id)).map(_.id)

            c(PartitionResponse(id, CONFLICTED, APPLIED))
          })
        }
      }
    }.map(_ => Ack()).ensure {
        consumer.commit()
        consumer.resume()
    }
  }

  //consumer.handler(process)

  def process(cmd: GetOffset): Future[Command] = {
    Future.value(OffsetResult.apply(offset))
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: GetOffset => process(cmd)
      case cmd: Epoch => process(cmd)
    }
  }
}
