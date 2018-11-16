package cn.windylee.netty.discard;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;

public class DiscardClientHandler extends SimpleChannelInboundHandler {

    private ByteBuf content;
    private ChannelHandlerContext ctx;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
//        建立连接后向Server发送消息
        this.ctx = ctx;
        content = ctx.alloc().directBuffer(DiscardClient.SIZE)
                .writeZero(DiscardClient.SIZE);

        generateTraffic();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        content.release();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }

    private void generateTraffic() {
//        调用retain方法增加ByteBuf的引用计数
        ctx.writeAndFlush(content.duplicate().retain()).addListener(trafficGenerator);
    }

    private final ChannelFutureListener trafficGenerator = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) generateTraffic();
            else {
                future.cause().printStackTrace();
                future.channel().close();
            }
        }
    };

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

    }
}
