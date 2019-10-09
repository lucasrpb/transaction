import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}
import com.google.protobuf.any.Any
import com.twitter.finagle.Service
import com.twitter.finagle.client.StackClient
import com.twitter.finagle.dispatch.SerialClientDispatcher
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.util.{Future, Promise}
import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}
import transaction.protocol._

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

package object transaction {

  val TIMEOUT = 1000L
  
  object Status {
    val ABORTED = 0
    val COMMITTED = 1
    val PENDING = 2
  }

  val PARTITIONS = 5

  val accounts = TrieMap[String, Long]()

  implicit def sfToTwitterFuture[T](tf: scala.concurrent.Future[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]
    tf.onComplete {
      case Success(r) => p.setValue(r)
      case Failure(e) => p.setException(e)
    }
    p
  }

  implicit def rsfToScalaFuture[T](rsf: ListenableFuture[T])(implicit ec: ExecutionContext): Future[T] = {
    val p = Promise[T]()

    Futures.addCallback(rsf, new FutureCallback[T] {
      override def onSuccess(result: T): Unit = {
        p.setValue(result)
      }

      override def onFailure(t: Throwable): Unit = {
        p.setException(t)
      }
    }, ec.asInstanceOf[java.util.concurrent.Executor])

    p
  }

  def createConnection(host: String, port: Int): Service[Command, Command] = {
    val addr = new java.net.InetSocketAddress(host, port)
    val transporter = Netty4Transporter.raw[Command, Command](TransactorClient.NullDelimiterPipeline, addr,
      StackClient.defaultParams)

    val bridge: Future[Service[Command, Command]] = transporter() map { transport =>
      new SerialClientDispatcher[Command, Command](transport)
    }

    (req: Command) =>
      bridge flatMap { svc =>
        svc(req) //ensure svc.close()
      }
  }

  /*case class Transaction(id: String, val e: Enqueue, var tmp: Long){
    val p = Promise[Command]()
  }*/

  final class CommandEncoder extends MessageToMessageEncoder[Command] {
    override def encode(ctx: ChannelHandlerContext, msg: Command, out: java.util.List[AnyRef]): Unit = {

      val buf = ctx.alloc().buffer().retain()

      msg match {
        case cmd: Ack => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Nack => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: ReadRequest => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: ReadResponse => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Transaction => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Batch => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: MVCCVersion => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: CoordinatorResult => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: TxList => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: KeyList => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: BatchInfo => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Epoch => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: BatchStart => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: BatchDone => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
      }

      buf.release()
    }
  }

  final class CommandDecoder extends MessageToMessageDecoder[ByteBuf] {
    override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: java.util.List[AnyRef]): Unit = {

      val bytes = ByteBufUtil.getBytes(msg).array
      val p = Any.parseFrom(bytes)

      p match {
        case _ if p.is(Ack) => out.add(p.unpack(Ack))
        case _ if p.is(Nack) => out.add(p.unpack(Nack))
        case _ if p.is(ReadRequest) => out.add(p.unpack(ReadRequest))
        case _ if p.is(ReadResponse) => out.add(p.unpack(ReadResponse))
        case _ if p.is(Transaction) => out.add(p.unpack(Transaction))
        case _ if p.is(Batch) => out.add(p.unpack(Batch))
        case _ if p.is(MVCCVersion) => out.add(p.unpack(MVCCVersion))
        case _ if p.is(CoordinatorResult) => out.add(p.unpack(CoordinatorResult))
        case _ if p.is(TxList) => out.add(p.unpack(TxList))
        case _ if p.is(Epoch) => out.add(p.unpack(Epoch))
        case _ if p.is(KeyList) => out.add(p.unpack(KeyList))
        case _ if p.is(BatchInfo) => out.add(p.unpack(BatchInfo))
        case _ if p.is(BatchStart) => out.add(p.unpack(BatchStart))
        case _ if p.is(BatchDone) => out.add(p.unpack(BatchDone))
      }

    }
  }

}
