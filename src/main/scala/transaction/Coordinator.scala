package transaction

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask, UUID}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

import com.datastax.driver.core.{Cluster, Session}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future, Promise}
import io.vertx.scala.core.Vertx
import io.vertx.scala.kafka.client.producer.{KafkaProducer, KafkaProducerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

class Coordinator(val id: String, val host: String, val port: Int)(implicit val ec: ExecutionContext)
  extends Service[Command, Command]{

  val resolver = createConnection("127.0.0.1", 2552)

  val cluster = Cluster.builder()
    .addContactPoint("127.0.0.1")
    .build()

  val session = cluster.connect("mvcc")

  case class Request(id: String, t: Transaction, tmp: Long = System.currentTimeMillis()){
    val p = Promise[Command]()

    val rs: Seq[String] = t.rs.map(_._1).toSeq
    val ws: Seq[String] = t.ws.map(_._1).toSeq

    val partitions = (rs ++ ws).distinct.map(k => (k.toInt % NPARTITIONS).toString)
  }

  val batch = new ConcurrentLinkedQueue[Request]()
  val executing = TrieMap[String, Request]()

  val timer = new Timer()
  val done = new AtomicBoolean(true)

  timer.scheduleAtFixedRate(new TimerTask {
    override def run(): Unit = {

      val now = System.currentTimeMillis()

      /*if(!executing.isEmpty) {
        executing.filter(now - _._2.tmp >= TIMEOUT).foreach { case (_, r) =>
          executing.remove(r.id)
          r.p.setValue(Nack())
        }
        return
      }*/

      if(!done.get()) return

      var txs = Seq.empty[Request]
      val it = batch.iterator()

      while(it.hasNext()){
        txs = txs :+ it.next()
      }

      var keys = executing.map(_._2.rs).flatten.toSeq

      txs.sortBy(_.id).foreach { r =>
        val elapsed = now - r.tmp

        if(elapsed >= TIMEOUT){
          batch.remove(r)
          r.p.setValue(Nack())
        } else if(!r.rs.exists(keys.contains(_))){
          keys = keys ++ r.rs
          batch.remove(r)
          executing.put(r.id, r)
        }
      }

      request()
    }
  }, 10L, 10L)

  def process(t: Transaction): Future[Command] = {

    if(batch.size() >= 10000){
      return Future.value(Nack())
    }

    val req = Request(t.id, t)

    batch.offer(req)
    req.p
  }

  def process(r: Read): Future[Command] = {
    Future.collect(r.keys.map{k => read(k)}).map(result => ReadResult(result.toMap))
  }

  def read(k: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind.setString(0, k)).map { r =>
      val one = r.one()
      k -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  val READ_TRANSACTION = session.prepare("select * from transactions where id=?;")
  val UPDATE_DATA = session.prepare("update data set value=?, version=? where key=?;")
  val READ_DATA = session.prepare("select * from data where key=?;")
  val UPDATE_TRANSACTION = session.prepare("update transactions set status=? where id=? if status=?")

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

  def process(pr: PartitionResponse): Future[Command] = {

    println(s"partition response from ${pr.id} with allowed partitions ${pr.allowed}\n")

    val allowed = pr.allowed

    if(allowed.isEmpty) {
      resolver(PartitionRelease.apply(id, allowed))
      return Future.value(Nack())
    }

    var keys = Seq.empty[String]
    val now = System.currentTimeMillis()

    val TXS = executing.filter { case (id, r) =>
      val elapsed = now - r.tmp

      if(elapsed >= TIMEOUT){
        executing.remove(id)
        r.p.setValue(Nack())
        false
      } else if(!r.rs.exists(keys.contains(_))){
        keys = keys ++ r.rs
        true
      } else {
        false
      }
    }

    // Executing all txs that spam allowed partitions
    val txs = TXS.filter { case (_, r) =>
      !r.partitions.exists(!allowed.contains(_))
    }.map(_._2.t).toSeq

    val release = PartitionRelease(id, allowed)

    var conflicted = Seq.empty[(Transaction, Boolean)]
    var accepted = Seq.empty[(Transaction, Boolean)]

    def clean(): Unit = {
      conflicted.foreach { case (t, _) =>
        executing.remove(t.id).get.p.setValue(Nack())
      }

      accepted.foreach { case (t, _) =>
        executing.remove(t.id).get.p.setValue(Ack())
      }
    }

    Future.collect(txs.map(t => checkTx(t).map(t -> _))).flatMap { checks =>
      conflicted = checks.filter(_._2 == false)
      accepted = checks.filter(_._2 == true)

      // Assuming nothing goes wrong...
      Future.collect(accepted.map{case (t, _) => writeTx(t)}).map { writes =>

        clean()
        resolver(release)

        Ack()
      }
    }.onFailure { case t =>

      t.printStackTrace()

      clean()
      resolver(release)

      Future.value(Ack())
    }
  }

  def request(): Unit = {

    done.set(false)

    val partitions = executing.values.map(_.partitions).toSeq.flatten.distinct

    if(partitions.isEmpty) {
      done.set(true)
      return
    }

    //println(s"partitions for coordinator ${id} ${partitions}\n")

    val req = PartitionRequest.apply(id, partitions)

    resolver(req)/*.flatMap { response =>
      process(response.asInstanceOf[PartitionResponse])
    }.handle { case t =>
      t.printStackTrace()
    }.ensure {
      done.set(true)
    }*/
  }

  override def apply(request: Command): Future[Command] = {
    request match {
      case cmd: Transaction => process(cmd)
      case cmd: PartitionResponse =>

        process(cmd).map { r =>
          done.set(true)
          r
        }
      case cmd: Read => process(cmd)
    }
  }

  //Await.ready(producer.closeFuture(), 60 seconds)
}
