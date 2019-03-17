package cn.windylee.netty.factorial;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.math.BigInteger;
import java.util.List;

//继承ByteToMessageDecoder类用于将字节转换为Java中的对象
public class BigIntegerDecoder extends ByteToMessageDecoder {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 5) return;
//        标记读下标为止
        in.markReaderIndex();
        int magicNumber = in.readUnsignedByte();
        if (magicNumber != 'F') {
//            重置读下标到之前标记的位置
            in.resetReaderIndex();
            throw new CorruptedFrameException("Invalid magic number: " + magicNumber);
        }
        int dataLength = in.readInt();
        if (in.readableBytes() < dataLength) {
            in.resetReaderIndex();
            return;
        }
        byte[] decoded = new byte[dataLength];
//        读取in中的数据写入到decoded字节数组中，原ByteBuf的读下标同样向前移动
        in.readBytes(decoded);
        out.add(new BigInteger(decoded));
    }

}
