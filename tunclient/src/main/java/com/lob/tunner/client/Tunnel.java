package com.lob.tunner.client;

import com.lob.tunner.BlockUtils;
import com.lob.tunner.common.Block;
import com.lob.tunner.common.Config;
import com.lob.tunner.logger.AutoLog;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents one multiplexed TCP tunnel to the remote tunnel server.
 */
public class Tunnel {
    private static final AtomicInteger _COUNTER = new AtomicInteger();

    private final int _id;
    private volatile SocketChannel _channel;
    private final ConcurrentHashMap<Integer, Connection> _connections = new ConcurrentHashMap<>();

    // Framing state machine for incoming stream
    private static final int _RS_HEADER = 1;
    private static final int _RS_PAYLOAD = 2;
    private static final int _RS_PADDING = 3;
    private int _readState = _RS_HEADER;

    private final ByteBuffer _header = ByteBuffer.allocate(8);
    private ByteBuffer _data = ByteBuffer.allocate(1024);
    private final ByteBuffer _padding = ByteBuffer.allocate(8);

    // Rate tracking & Noise generator
    private final AtomicLong _realBytesSentInTick = new AtomicLong(0);
    private ScheduledExecutorService _noiseScheduler;
    private static NoiseSource _noiseSource;

    public Tunnel() {
        _id = _COUNTER.incrementAndGet();
    }

    public int getID() {
        return _id;
    }

    public void multiplex(Connection conn) {
        _connections.put(conn.getID(), conn);
    }

    public void remove(int connId) {
        _connections.remove(connId);
    }

    public boolean overloaded() {
        return _connections.size() > 10;
    }

    public void start(String target, int port, int forwardPort) throws IOException {
        start(target, port);
    }

    public void start(String target, int port) throws IOException {
        AutoLog.INFO.log("Starting new Netty TCP tunnel to %s:%d ...", target, port);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(TunnelClient.REMOTEWORKER)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new TunnelInboundHandler());
                    }
                });

        try {
            ChannelFuture future = bootstrap.connect(target, port).sync();
            _channel = (SocketChannel) future.channel();
            AutoLog.INFO.log("Tunnel %08x connected successfully to %s:%d", _id, target, port);
        } catch (Exception e) {
            AutoLog.ERROR.exception(e).log("Failed to connect tunnel to %s:%d", target, port);
            throw new IOException("Cannot connect to remote tunnel server: " + e.getMessage(), e);
        }

        _startNoiseGenerator();
    }

    private void _startNoiseGenerator() {
        long minRate = Config.getMinRate();
        if (minRate <= 0) {
            return;
        }

        synchronized (Tunnel.class) {
            if (_noiseSource == null) {
                _noiseSource = new NoiseSource(Config.getNoiseFile());
            }
        }

        final int tickMs = (minRate < 1000) ? 500 : 100;
        final long targetBytesPerTick = Math.max(1, (minRate * tickMs) / 1000);

        AutoLog.INFO.log("Starting noise generator for tunnel %08x: minRate=%d bytes/s (~%d bytes per %d ms), echo=%s",
                _id, minRate, targetBytesPerTick, tickMs, Config.isEcho());

        _noiseScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "NoiseGenerator-" + _id);
            t.setDaemon(true);
            return t;
        });

        _noiseScheduler.scheduleAtFixedRate(() -> {
            try {
                if (_channel == null || !_channel.isActive()) {
                    return;
                }

                long realSent = _realBytesSentInTick.getAndSet(0);
                if (realSent < targetBytesPerTick) {
                    long deficit = targetBytesPerTick - realSent;
                    short ctrlCode = Config.isEcho() ? Block.CODE_ECHO : Block.CODE_NOISE;

                    while (deficit > 0) {
                        int chunkSize = (int) Math.min(deficit, Block.MAX_NO_PADDING_LENGTH);
                        ByteBuffer noiseBuf = _noiseSource.getBytes(chunkSize);
                        Block block = new Block(0, BlockUtils.control(ctrlCode), chunkSize, noiseBuf);
                        _sendBlock(block, false);
                        deficit -= chunkSize;
                    }
                }
            } catch (Exception e) {
                AutoLog.ERROR.exception(e).log("Error generating noise on tunnel %08x", _id);
            }
        }, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    public void write(Block block) {
        _sendBlock(block, true);
    }

    private void _sendBlock(Block block, boolean isRealTraffic) {
        if (_channel == null || !_channel.isActive()) {
            AutoLog.ERROR.log("Tunnel %08x channel is closed, cannot write block!", _id);
            return;
        }

        int length = block.length();
        if (block.type() == Block.BLOCK_DATA) {
            TunnelManager.TotalWrite.addAndGet(length);
            if (isRealTraffic) {
                _realBytesSentInTick.addAndGet(length);
            }
            AutoLog.INFO.log(
                    "Write out data block (seq=%d, length=%d) for connection %08x on tunnel %08x ...",
                    block.sequence(), length, block.connection(), _id
            );
        } else {
            if (isRealTraffic) {
                TunnelManager.TotalWrite.addAndGet(length);
            }
        }

        int padding = (length % 8 == 0) ? 0 : (8 - (length % 8));
        ByteBuf buf = _channel.alloc().buffer(8 + length + padding);

        // Header
        buf.writeShort(block.getTypeSeq());
        buf.writeShort((short) (length & 0xFFFF));
        buf.writeInt(block.connection());

        // Payload
        if (length > 0 && block.data() != null) {
            ByteBuffer data = block.data();
            data.rewind();
            buf.writeBytes(data);
        }

        // Padding
        if (padding > 0) {
            buf.writeBytes(BlockUtils.PADDING, 0, padding);
        }

        _channel.writeAndFlush(buf);
    }

    private boolean _readInto(ByteBuf src, ByteBuffer dst) {
        while (dst.hasRemaining() && src.isReadable()) {
            dst.put(src.readByte());
        }
        return dst.hasRemaining();
    }

    public synchronized void read(ByteBuf data) {
        while (data.isReadable()) {
            if (_readState == _RS_HEADER) {
                if (_readInto(data, _header)) {
                    return; // need more data
                }

                _header.rewind();
                short typeSeq = _header.getShort();
                int len = (_header.getShort() & 0xFFFF);
                int conn = _header.getInt();

                if (typeSeq == 0 && len == 0 && conn == 0) {
                    _header.clear();
                    continue;
                }

                if (len > 0) {
                    while (len > _data.capacity()) {
                        _data = ByteBuffer.allocate(_data.capacity() << 1);
                    }
                    _data.rewind();
                    _data.limit(len);
                    _readState = _RS_PAYLOAD;
                } else {
                    _handleBlock(conn, typeSeq, 0, null);
                    _header.clear();
                    _readState = _RS_HEADER;
                }
            } else if (_readState == _RS_PAYLOAD) {
                if (_readInto(data, _data)) {
                    return; // need more data
                }

                int len = _data.limit();
                int padding = len % 8;
                if (padding > 0) {
                    _padding.rewind();
                    _padding.limit(8 - padding);
                    _readState = _RS_PADDING;
                } else {
                    _data.rewind();
                    _handleBlock(_header.getInt(4), _header.getShort(0), len, _data);
                    _header.clear();
                    _readState = _RS_HEADER;
                }
            } else if (_readState == _RS_PADDING) {
                if (_readInto(data, _padding)) {
                    return; // need more data
                }

                _data.rewind();
                _handleBlock(_header.getInt(4), _header.getShort(0), _data.limit(), _data);
                _header.clear();
                _readState = _RS_HEADER;
            }
        }
    }

    private void _handleBlock(int connId, short typeSeq, int len, ByteBuffer payload) {
        Block block;
        if (len == 0 || payload == null) {
            block = new Block(connId, typeSeq);
        } else {
            ByteBuffer slice = ByteBuffer.allocate(len);
            slice.put(payload);
            slice.rewind();
            block = new Block(connId, typeSeq, len, slice);
        }

        if (block.type() == Block.BLOCK_DATA) {
            TunnelManager.TotalRead.addAndGet(block.length());
            AutoLog.INFO.log(
                    "Read in data block (seq=%d, length=%d) for connection %08x on tunnel %08x ...",
                    block.sequence(), block.length(), connId, _id
            );

            Connection conn = _connections.get(connId);
            if (conn == null) {
                AutoLog.WARN.log("Found non-existing connection %08x", connId);
                write(new Block(connId, BlockUtils.control(Block.CODE_ABORT)));
                return;
            }
            try {
                conn.respond(block);
            } catch (Exception e) {
                AutoLog.ERROR.exception(e).log("Failed responding to connection %08x", connId);
            }
        } else {
            TunnelManager.getInstance().handleControl(this, connId, block.control(), block.data());
        }
    }

    public void shutdown() {
        if (_noiseScheduler != null) {
            _noiseScheduler.shutdownNow();
            _noiseScheduler = null;
        }

        for (Connection conn : _connections.values()) {
            conn.shutdown();
        }
        _connections.clear();

        if (_channel != null) {
            try {
                _channel.close();
            } catch (Exception ignored) {
            }
            _channel = null;
        }
    }

    class TunnelInboundHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf buf = (ByteBuf) msg;
            try {
                read(buf);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            AutoLog.ERROR.exception(cause).log("Tunnel %08x inbound exception", _id);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            AutoLog.WARN.log("Tunnel %08x disconnected from server", _id);
            shutdown();
            super.channelInactive(ctx);
        }
    }
}
