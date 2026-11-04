package com.lob.tunner.client;

import com.lob.tunner.BlockUtils;
import com.lob.tunner.common.Block;
import com.lob.tunner.common.Config;
import com.lob.tunner.logger.AutoLog;
import io.netty.channel.Channel;

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

    private static final TunnelManager _INST = new TunnelManager();
    public static TunnelManager getInstance() {
        return _INST;
    }

    private final ConcurrentHashMap<Integer, Connection> _connections = new ConcurrentHashMap<>();
    private final ArrayList<Tunnel> _tunnels = new ArrayList();

    public void start() {
        AutoLog.INFO.log("Starting Tunnel Manager ...");
    }

    /**
     * 1. assign the connection to one of the tunnel
     * 2. If no tunnel available, let's create one tunnel
     * 3. If tunnel's threshold met, let's create a new tunnel
     * @param conn
     */
    public synchronized void accept(Connection conn, Channel channel) throws IOException {
        Tunnel tunnel = _findTunnel();

        if(tunnel == null) {
            tunnel = _createTunnel();
        }

        if(tunnel == null) {
            throw new IOException("Cannot find tunnel to multiplexing connection - " + conn.getID());
        }

        AutoLog.INFO.log("Starting one new connection %08x (channel=%s) on tunnel %08x ...", conn.getID(), channel, tunnel.getID());
        _connections.put(conn.getID(), conn);
        tunnel.multiplex(conn);
        conn.attach(tunnel);

        // start the connection on this tunnel
        tunnel.write(new Block(conn.getID(), BlockUtils.control(Block.CODE_START)));
    }


    /**
     * Close a connection
     *
     * @param conn
     */
    public void close(Connection conn) {
        int connId = conn.getID();

        _connections.remove(connId);

        Tunnel tunnel = conn.tunnel();

        AutoLog.INFO.log("Shutting one connection %08x on tunnel %08x ...", conn.getID(), tunnel.getID());

        // notify server ...
        tunnel.write(new Block(connId, BlockUtils.control(Block.CODE_ABORT)));
        tunnel.remove(connId);

        conn.shutdown();
    }

    /**
     *
     * @param tunnel
     * @param connId
     * @param control
     * @param data
     */
    public void handleControl(Tunnel tunnel, int connId, short control, ByteBuffer data) {
        AutoLog.INFO.log("Handling control %04x for connection %08x ...", control, connId);

        Connection conn;
        switch(control) {
            case Block.CODE_ABORT:
                tunnel.remove(connId);

                conn = _connections.remove(connId);
                if(conn == null) {
                    AutoLog.ERROR.log("Closing non-existing connection %08x", connId);
                }
                else {
                    conn.shutdown();
                }

                break;
            case Block.CODE_ECHO:
            case Block.CODE_NOISE:
                // Server echoed back noise or dummy packet; log and discard (do not re-echo)
                AutoLog.DEBUG.log("Received dummy/echo packet from server (%d bytes)", data != null ? data.remaining() : 0);
                break;
            case Block.CODE_RESUME:
                // shall not happen, only used at client side to resume data ...
                break;
            default:
                break;
        }
    }

    private final Tunnel _findTunnel() {
        for(int idx = 0; idx < _tunnels.size(); idx ++) {
            Tunnel tunnel = _tunnels.get(idx);
            if(!tunnel.overloaded()) {
                return tunnel;
            }
        }

        return null;
    }

    private final Tunnel _createTunnel() throws IOException {
        Tunnel tunnel = new Tunnel();

        tunnel.start(Config.getServerAddress(), Config.getServerPort(), Config.getForwardPort());
        _tunnels.add(tunnel);

        return tunnel;
    }

    private void _timeout() {
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

                _timeout();

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
