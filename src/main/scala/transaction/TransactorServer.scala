package transaction

import com.twitter.finagle.dispatch.SerialServerDispatcher
import com.twitter.finagle.netty4.Netty4Listener
import com.twitter.finagle.server.{StackServer, StdStackServer}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.{param, _}
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}

object TransactorServer {
  val protocolLibrary = "string"

  private object StringServerPipeline extends (ChannelPipeline => Unit) {
    def apply(pipeline: ChannelPipeline): Unit = {

      pipeline.addLast("line", new DelimiterBasedFrameDecoder(1 * 1024 * 1024, Delimiters.nulDelimiter(): _*))
      //pipeline.addLast("stringDecoder", new StringDecoder(StandardCharsets.UTF_8))
      //pipeline.addLast("stringEncoder", new StringEncoder(StandardCharsets.UTF_8))

      pipeline.addLast("commandDecoder", new CommandDecoder())
      pipeline.addLast("commandEncoder", new CommandEncoder())
    }
  }

  case class Server(
                     stack: Stack[ServiceFactory[Command, Command]] = StackServer.newStack,
                     params: Stack.Params = StackServer.defaultParams + param.ProtocolLibrary(protocolLibrary))
    extends StdStackServer[Command, Command, Server] {
    protected def copy1(
                         stack: Stack[ServiceFactory[Command, Command]] = this.stack,
                         params: Stack.Params = this.params
                       ) = copy(stack, params)

    protected type In = Command
    protected type Out = Command
    protected type Context = TransportContext

    protected def newListener() = Netty4Listener(StringServerPipeline, params)
    protected def newDispatcher(
                                 transport: Transport[In, Out] { type Context <: Server.this.Context },
                                 service: Service[Command, Command]
                               ) = new SerialServerDispatcher(transport, service)
  }

  def server: Server = Server()
}

