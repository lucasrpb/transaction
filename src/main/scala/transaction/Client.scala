package transaction

import com.datastax.driver.core.Session
import com.twitter.util.Future
import transaction.protocol._

import scala.concurrent.ExecutionContext

class Client(val id: String, val session: Session)(implicit val ec: ExecutionContext) {

  val INSERT_TRANSACTION = session.prepare("insert into transactions(id, status, tmp) values(?,?,?);")
  val READ_DATA = session.prepare("select * from data where key=?;")

  def read(k: String): Future[(String, VersionedValue)] = {
    session.executeAsync(READ_DATA.bind()).map { r =>
      val one = r.one()
      k -> VersionedValue(one.getString("version"), one.getLong("value"))
    }
  }

  def insertTx(): Future[Boolean] = {
    val tmp = System.currentTimeMillis()
    session.executeAsync(INSERT_TRANSACTION.bind.setString(0, id).setInt(1, Status.PENDING).setLong(2, tmp))
      .map(_.wasApplied())
  }

  def reads(keys: Seq[String], f: (Map[String, VersionedValue]) => Map[String, VersionedValue]): Future[Boolean] = {
    Future.collect(keys.map{k => read(k)}).flatMap { reads =>
      val writes = f(reads)

      null
    }
  }

  def execute(keys: Seq[String])(f: (Map[String, VersionedValue]) => Map[String, VersionedValue]): Future[Boolean] = {
    insertTx().flatMap { ok =>
      if(ok){
        reads(keys, f)
      } else {
        Future.value(false)
      }
    }
  }

}
