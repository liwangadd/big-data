package cn.windylee.netty.timeserver.ByteBufImpl;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

public class TimeDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
//        ByteToMessageDecoder内部维护了一个缓存ByteBuf
        if (in.readableBytes() < 4) {
//            直接返回之后，将数据追加到缓存中，数据到来会继续调用decode函数
            return;
        }
//        将缓存中的数据添加到out中后，会自动清空缓存ByteBuf
        out.add(in.readBytes(4));
    }
}
