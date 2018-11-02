package cn.windylee.netty.timeserver.ByteBufImpl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class TimeServerHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        final ByteBuf time = ctx.alloc().buffer(4);
        time.writeInt((int) ((int) System.currentTimeMillis() / 1000L + 2208988800L));

        final ChannelFuture f = ctx.writeAndFlush(time);

        f.addListener(ChannelFutureListener.CLOSE);
//        netty所有的操作都是异步，直接调用ctx.close()方法可能在数据没有发送结束就关闭了输出流
//        给ChannelFuture添加监听器，在监听器收到消息后关闭输出流
//        f.addListener(future -> {
//            assert f == future;
//            ctx.close();
//        });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
