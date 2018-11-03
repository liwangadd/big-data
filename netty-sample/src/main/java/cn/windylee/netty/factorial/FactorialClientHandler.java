package cn.windylee.netty.factorial;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.math.BigInteger;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class FactorialClientHandler extends SimpleChannelInboundHandler<BigInteger> {

    private ChannelHandlerContext ctx;
    private int receivedMessages;
    private int next = 1;
    final BlockingQueue<BigInteger> answer = new LinkedBlockingDeque<>();

    public BigInteger getFactorial() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    return answer.take();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.ctx = ctx;
        sendNumbers();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }

    private void sendNumbers() {
        ChannelFuture future = null;
        for (int i = 0; i < 4096 && next <= FactorialClient.COUNT; ++i) {
            future = ctx.write(next++);
        }
        if (next <= FactorialClient.COUNT) {
            assert future != null;
            future.addListener(numberSender);
        }
        ctx.flush();
    }

    private final ChannelFutureListener numberSender = future -> {
        if (future.isSuccess()) sendNumbers();
        else {
            future.cause().printStackTrace();
            future.channel().close();
        }
    };

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, BigInteger msg) {
        receivedMessages++;
        if (receivedMessages == FactorialClient.COUNT) {
            ctx.channel().close().addListener((ChannelFutureListener) future -> {
                boolean offered = answer.offer(msg);
                assert offered;
            });
        }
    }
}
