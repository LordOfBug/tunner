package com.lob.tunner.server;

import com.lob.tunner.BlockUtils;
import com.lob.tunner.common.Block;
import com.lob.tunner.common.Config;
import com.lob.tunner.logger.AutoLog;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tunnel are using SSHJ, which cannot use netty, we'll have to rely on native java Sockets
 *
 * To simplify the implementation, let's create a pair of threads per tunnel to handle the job like
 * 1. encode / decode client data
 * 2. I/O handling
 * 3. background handling like provide faked data, etc.
 */
public class TunnelManager {
    private TunnelManager() {
        new TimeoutThread().start();
    }

    public static AtomicLong TotalRead = new AtomicLong(0);
    public static AtomicLong TotalWrite = new AtomicLong(0);
    public final static EventLoopGroup CONNWORKERS = new NioEventLoopGroup(1);

    private static final TunnelManager _INST = new TunnelManager();
    public static TunnelManager getInstance() {
        return _INST;
    }

    private final ArrayList<Tunnel> _tunnels = new ArrayList();
    private final ConcurrentHashMap<Integer, Connection> _connections = new ConcurrentHashMap<>();

    public void start() {
        AutoLog.INFO.log("Starting Tunnel Manager ...");
    }

    /**
     * 1. assign the connection to one of the tunnel
     * 2. If no tunnel available, let's create one tunnel
     * 3. If tunnel's threshold met, let's create a new tunnel
     * @param tunnel
     */
    public synchronized void accept(Tunnel tunnel) throws IOException {
        AutoLog.INFO.log("New tunnel %08x created by client ...", tunnel.identifier());
        _tunnels.add(tunnel);
    }

    public void close(Connection conn) {
        _connections.remove(conn.identifier());

        Tunnel tunnel = conn.tunnel();
        tunnel.write(new Block(conn.identifier(), BlockUtils.control(Block.CODE_ABORT)));
        tunnel.remove(conn);

        conn.shutdown();
    }

    /**
     * Handle all control blocks received!!!
     *
     * @param tunnel
     * @param conId
     * @param control
     * @param data
     */
    public void handleControlBlock(Tunnel tunnel, int conId, short control, ByteBuffer data) {
        AutoLog.DEBUG.log("Handling control %04x for connection %08x on tunnel %08x...", control, conId, tunnel.identifier());

        switch(control) {
            case Block.CODE_START:
                /**
                 * A new connection be started on this tunnel!!!
                 */
                if(_connections.containsKey(conId)) {
                    // dame!
                    AutoLog.ERROR.log("Connection with ID already exists %08x", conId);
                    throw new RuntimeException(String.format("Found already existing connection %08x", conId));
                }
                else {
                    AutoLog.INFO.log("Starting one new connection %08x on tunnel %08x ...", conId, tunnel.identifier());
                    Connection conn = new Connection(conId);
                    _connections.put(conId, conn);

                    // try connect to target
                    conn.connect(Config.getProxyAddress(), Config.getProxyPort());

                    // and allow multiplexing on this tunnel
                    tunnel.multiplex(conn);
                }
                break;
            case Block.CODE_RESUME:
                if(!_connections.containsKey(conId)) {
                    AutoLog.ERROR.log("Resuming non-existing connection %08x", conId);
                    throw new RuntimeException(String.format("Try resuming non-existing connection %08x", conId));
                }

                // we need to abort from old tunnel if there's and associate it with new one
                // TODO
                AutoLog.ERROR.log("Resuming connection on different tunnel not supported!!!");
                tunnel.write(new Block(conId, BlockUtils.control(Block.CODE_ABORT)));
                break;
            case Block.CODE_ABORT:
                if(!_connections.containsKey(conId)) {
                    AutoLog.WARN.log("Abort non-existing connection %08x on tunnel %08x", conId, tunnel.identifier());
                    break;
                }
                else {
                    AutoLog.INFO.log("Shutting down one connection %08x on tunnel %08x ...", conId, tunnel.identifier());
                    Connection conn = _connections.remove(conId);
                    tunnel.remove(conn);
                    conn.shutdown();
                }
                break;
            case Block.CODE_NOISE:
                // Dummy/noise data from client: discard payload silently to save bandwidth
                if (data != null) {
                    TunnelManager.TotalRead.addAndGet(data.remaining());
                    AutoLog.DEBUG.log("Discarded noise block of %d bytes on tunnel %08x", data.remaining(), tunnel.identifier());
                }
                break;
            case Block.CODE_ECHO:
                if (data != null && data.hasRemaining()) {
                    TunnelManager.TotalRead.addAndGet(data.remaining());
                    ByteBuffer echoData = ByteBuffer.allocate(data.remaining());
                    echoData.put(data);
                    echoData.rewind();
                    tunnel.write(new Block(0, BlockUtils.control(Block.CODE_ECHO), echoData.remaining(), echoData));
                } else {
                    tunnel.write(new Block(0, BlockUtils.control(Block.CODE_ECHO)));
                }
                break;
            default:
                break;
        }
    }

    private void _checkTimeouts() {
        // for connections without active for more than 30 seconds, let's kill them ...
        long now = System.currentTimeMillis();
        long timeout = now - 30 * 1000;

        _connections.values().forEach(conn -> {
            if(conn.lastRead() < timeout && conn.lastWrite() < timeout) {
                conn.timeout(now);
            }
        });
    }

    class TimeoutThread extends Thread {
        @Override
        public void run() {
            while(true) {
                try {
                    Thread.sleep(30 * 1000);
                }
                catch(InterruptedException ie) {
                    // ignore
                }

                _checkTimeouts();

                System.out.println(String.format(
                        "=====================================================================\n" +
                                " Total Connections=%d, Total Read=%d bytes, Total Write=%d bytes\n" +
                                "=====================================================================",
                        _connections.size(), TotalRead.getAndSet(0), TotalWrite.getAndSet(0)
                ));
            }
        }
    }
}
