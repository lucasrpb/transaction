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

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

package object transaction {

  val TIMEOUT = 1000L

  object Status {
    val ABORTED = 0
    val COMMITTED = 1
    val PENDING = 2
  }

  val NPARTITIONS = 400

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
        case cmd: Read => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: ReadResult => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Transaction => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: Batch => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: VersionedValue => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: PartitionResponse => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: PartitionRequest => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
        case cmd: PartitionRelease => out.add(buf.writeBytes(Any.pack(cmd).toByteArray))
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
        case _ if p.is(Read) => out.add(p.unpack(Read))
        case _ if p.is(ReadResult) => out.add(p.unpack(ReadResult))
        case _ if p.is(Transaction) => out.add(p.unpack(Transaction))
        case _ if p.is(Batch) => out.add(p.unpack(Batch))
        case _ if p.is(VersionedValue) => out.add(p.unpack(VersionedValue))
        case _ if p.is(PartitionRequest) => out.add(p.unpack(PartitionRequest))
        case _ if p.is(PartitionRelease) => out.add(p.unpack(PartitionRelease))
        case _ if p.is(PartitionResponse) => out.add(p.unpack(PartitionResponse))
      }

    }
  }

}
