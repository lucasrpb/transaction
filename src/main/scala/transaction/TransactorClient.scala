package transaction

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelPipeline}
import io.netty.handler.codec
import io.netty.handler.codec.Delimiters

object TransactorClient {

  val protocolLibrary = "string"

  object NullDelimiterPipeline extends (ChannelPipeline => Unit) {
    def apply(pipeline: ChannelPipeline): Unit = {

      pipeline.addLast("lineEncoder", new codec.MessageToMessageEncoder[ByteBuf] {
        override def encode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[AnyRef]): Unit = {

          msg.retain()
          msg.writeBytes(Delimiters.nulDelimiter()(0))

          out.add(msg)
        }
      } )

      pipeline.addLast("commandEncoder", new CommandEncoder())
      pipeline.addLast("commandDecoder", new CommandDecoder())

    }
  }

}
