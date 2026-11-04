package com.lob.tunner.common;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Config {
    /**
     * Initialize configuration from arguments
     * @param args
     */
    public static void initialize(String[] args, boolean isClient) {
        ArgumentParser parser = ArgumentParsers.newFor(isClient ? "TunClient" : "TunServer").build()
                .defaultHelp(true)
                .description(isClient ? "Tunnel client options" : "Tunnel server options");

        parser.addArgument("-i", "--interface").setDefault("127.0.0.1")
                .help("Default local interface to listen on, default to 127.0.0.1");

        parser.addArgument("-l", "--listen").type(Integer.class).setDefault(8080)
                .help("Default local port to listen on, default to 8080");

        if(isClient) {
            parser.addArgument("-r", "--remotePort").type(Integer.class).setDefault(8080)
                    .help("Remote tunnel server port to connect, default to 8080");

            parser.addArgument("-n", "--min-rate").setDefault("0")
                    .help("Guaranteed minimum data stream rate (e.g. 10k, 1m), default 0 (disabled)");

            parser.addArgument("--noise-file").setDefault("")
                    .help("File to use for streaming noise/dummy data (streamed repeatedly)");

            parser.addArgument("-e", "--echo").action(Arguments.storeTrue())
                    .help("Request server to echo back dummy/noise data (default: false, discard)");

            // Legacy / compatibility flags (optional)
            parser.addArgument("-u", "--user").setDefault("")
                    .help("User name (deprecated, kept for compatibility)");

            parser.addArgument("-k", "--key").setDefault("")
                    .help("Key file (deprecated, kept for compatibility)");

            parser.addArgument("-p", "--password").setDefault("")
                    .help("Password (deprecated, kept for compatibility)");

            parser.addArgument("-f", "--forwardPort").type(Integer.class).setDefault(8080)
                    .help("Remote forward port (deprecated, kept for compatibility)");

            parser.addArgument("server").nargs(1).required(true)
                    .help("Remote server address connect to for tunneling");
        }
        else {
            parser.addArgument("-m", "--mode").setDefault("prod")
                    .help("Mode running the server, could be 'test' or 'prod'");

            parser.addArgument("-s", "--server").setDefault("127.0.0.1")
                    .help("Remote proxy server address to forward tunneling connection, default to 127.0.0.1");

            parser.addArgument("-f", "--forwardPort").type(Integer.class).setDefault(8888)
                    .help("Remote proxy port to forward tunneling connection to, default to 8888");
        }

        Namespace ns = null;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        _listenAddress = ns.getString("interface");
        _listenPort = ns.getInt("listen");

        if(isClient) {
            _serverAddress = ns.getList("server").get(0).toString();
            _serverPort = ns.getInt("remotePort");
            _forwardPort = ns.getInt("forwardPort");

            _serverUser = ns.getString("user");
            _serverKey = ns.getString("key");
            _serverPass = ns.getString("password");

            String minRateStr = ns.getString("min_rate");
            _minRate = parseRate(minRateStr);

            _noiseFile = ns.getString("noise_file");
            Boolean echoVal = ns.getBoolean("echo");
            _echo = (echoVal != null && echoVal);
        }
        else {
            _mode = ns.getString("mode");
            _proxyAddress = ns.getString("server");
            _proxyPort = ns.getInt("forwardPort");
        }
    }

    public static long parseRate(String rateStr) {
        if(rateStr == null || rateStr.trim().isEmpty()) {
            return 0;
        }
        rateStr = rateStr.trim().toLowerCase();
        long multiplier = 1;
        if(rateStr.endsWith("kb")) {
            multiplier = 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 2).trim();
        } else if(rateStr.endsWith("k")) {
            multiplier = 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 1).trim();
        } else if(rateStr.endsWith("mb")) {
            multiplier = 1024L * 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 2).trim();
        } else if(rateStr.endsWith("m")) {
            multiplier = 1024L * 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 1).trim();
        } else if(rateStr.endsWith("gb")) {
            multiplier = 1024L * 1024L * 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 2).trim();
        } else if(rateStr.endsWith("g")) {
            multiplier = 1024L * 1024L * 1024L;
            rateStr = rateStr.substring(0, rateStr.length() - 1).trim();
        }

        try {
            return (long)(Double.parseDouble(rateStr) * multiplier);
        } catch(NumberFormatException e) {
            return 0;
        }
    }

    private static long _minRate = 0;
    public static long getMinRate() {
        return _minRate;
    }

    private static String _noiseFile = "";
    public static String getNoiseFile() {
        return _noiseFile;
    }

    private static boolean _echo = false;
    public static boolean isEcho() {
        return _echo;
    }

    private static String _serverAddress = "proxy";
    public static String getServerAddress() {
        return _serverAddress;
    }

    private static int _serverPort = 8080;
    public static int getServerPort() {
        return _serverPort;
    }

    private static String _serverKey = "";
    public static String getServerKey() {
        return _serverKey;
    }

    private static String _serverUser = "";
    public static String getServerUser() {
        return _serverUser;
    }

    private static String _serverPass = "";
    public static String getServerPass() {
        return _serverPass;
    }

    private static int _forwardPort = 8080;
    public static int getForwardPort() {
        return _forwardPort;
    }

    private static String _listenAddress = "127.0.0.1";
    public static String getListenAddress() {
        return _listenAddress;
    }

    private static int _listenPort = 8080;
    public static int getListenPort() {
        return _listenPort;
    }

    private static String _proxyAddress = "127.0.0.1";
    public static String getProxyAddress() {
        return _proxyAddress;
    }

    private static int _proxyPort = 8888;
    public static int getProxyPort() {
        return _proxyPort;
    }

    private static String _mode = "prod";
    public static boolean isTestMode() {
        return "test".equalsIgnoreCase(_mode);
    }
}
