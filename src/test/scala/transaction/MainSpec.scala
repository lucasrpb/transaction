package transaction

import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

import com.datastax.driver.core.{Cluster, ResultSet, ResultSetFuture}
import com.twitter.util.{Await, Future}
import org.scalatest.FlatSpec

class MainSpec extends FlatSpec {

  "" should "" in {

    val cluster = Cluster.builder()
      .addContactPoint("127.0.0.1")
      .build();

    val session = cluster.connect("mvcc")

    val INSERT_DATA = session.prepare("insert into data(key, value, version) values(?,?,?);")

    var tasks = Seq.empty[Future[ResultSet]]

    val n = 1000

    val rand = ThreadLocalRandom.current()
    val MAX_VALUE = 1000L

    val tid = UUID.randomUUID.toString

    for(i<-0 until n){
      session.execute(INSERT_DATA.bind.setString(0, i.toString).setLong(1, rand.nextLong(0, MAX_VALUE)).setString(2, tid))
    }

    /*val READ_DATA = session.prepare("select sum(value) from data;")

    println(session.execute(READ_DATA.bind()).one().getLong(0))*/
  }

}
