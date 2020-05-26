package com.lob.tunner.server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.lob.tunner.common.Config;
import com.lob.tunner.logger.AutoLog;
import com.lob.tunner.server.db.AccountDao;
import com.lob.tunner.server.db.DaoUtils;
import com.lob.tunner.server.echo.EchoServer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.LoggerFactory;

/**
 * Create a listening port to accept client connections, read-in data and multiplexing the data on
 * one of the tunnels
 */
public class TunnelServer {
    public final static EventLoopGroup TUNWORKERS = new NioEventLoopGroup(1);

    private final static TunnelManager _tunnelManager = TunnelManager.getInstance();
    private final static EventLoopGroup BOSS = new NioEventLoopGroup(1);

    public static void main(String[] args) throws Exception {
        AutoLog.INFO.log("Disable logging ...");
        Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.OFF);

        Config.initialize(args, false);

        java.sql.Connection conn = DaoUtils.getConnection();
        AccountDao.get(conn, "foo", 0);
        
        try {
            if(Config.isTestMode()) {
                // for testing purpose
                AutoLog.INFO.log("Starting server in testing mode with an echo server running!");
                ChannelFuture future = EchoServer.start(Config.getProxyPort());
                /*
                future.addListener(new GenericFutureListener<Future<? super Void>>() {
                    @Override
                    public void operationComplete(Future<? super Void> future) throws Exception {
                        if(future instanceof ChannelFuture) {
                            ((ChannelFuture)future).channel().closeFuture().sync();
                        }
                    }
                });
                */
            }

            _tunnelManager.start();

            /**
             * Handle client APP connection.
             * When client APP connecting, we create a Connection object to wrap the socket channel, which will
             * 1. read data from client, forward the data to one of the tunnel (create if not existing)
             * 2. read data from assigned tunnel, forward the data to client
             */
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(BOSS, TUNWORKERS)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) {
                            channel.pipeline().addLast(new TunnelHandler(channel));
                        }
                    });

            int port = Config.getListenPort();

            ChannelFuture future = bootstrap.bind(port).sync();

            AutoLog.INFO.log("Starting at port " + port);

            future.channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            BOSS.shutdownGracefully();
            TUNWORKERS.shutdownGracefully();
        }
    }
}
