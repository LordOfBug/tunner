package com.lob.tunner.logger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AutoLog {
    private AutoLog() {

    }

    private static SimpleDateFormat _FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static String _timestamp() {
        return _FORMAT.format(new Date());
    }

    private static ILogger _impl = new ILogger() {
        boolean enable(int type) {
            return type >= ILogger.INFO;
        }

        ILogger message(int level, String msg, String context) {
            System.out.println(String.format("[%s] %3d %s", _timestamp(), level, msg));
            if(context != null) {
                System.out.println(context);
            }
            return this;
        }

        ILogger message(int level, String msg, String context, Throwable e) {
            System.out.println(String.format("[%s] %3d %s", _timestamp(), level, msg));
            if(context != null) {
                System.out.println(context);
            }
            e.printStackTrace(System.out);
            return this;
        }

        ILogger notice(int level, String cause, String action, String context) {
            System.out.println(String.format("[%s] %3d %s %s", _timestamp(), level, cause, action));
            if(context != null) {
                System.out.println(context);
            }

            return this;
        }

        ILogger notice(int level, String cause, String action, String context, Throwable e) {
            System.out.println(String.format("[%s] %3d %s %s", _timestamp(), level, cause, action));
            if(context != null) {
                System.out.println(context);
            }
            e.printStackTrace(System.out);
            return this;
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    };

    public static LogRecord record(int level) {
        if(_impl.enable(level)) {
            return LogRecord.loggerRecorder(_impl, level);
        }
        else {
            return LogRecord.dummyRecorder();
        }
    }

    public static LogRecord TRACE = record(ILogger.TRACE);
    public static LogRecord trace() {
        return record(ILogger.TRACE);
    }

    public static LogRecord DEBUG = record(ILogger.DEBUG);
    public static LogRecord debug() {
        return record(ILogger.DEBUG);
    }

    public static LogRecord INFO = record(ILogger.INFO);
    public static LogRecord info() {
        return record(ILogger.INFO);
    }

    public static LogRecord WARN = record(ILogger.WARN);
    public static LogRecord warn() {
        return record(ILogger.WARN);
    }

    public static LogRecord ERROR = record(ILogger.ERROR);
    public static LogRecord error() {
        return record(ILogger.ERROR);
    }

    public static LogRecord FATAL = record(ILogger.FATAL);
    public static LogRecord fatal() {
        return record(ILogger.FATAL);
    }

    /**
     * Testing purpose
     *
     * @param args
     */
    public static void main(String[] args) {
        AutoLog.TRACE.log("Hello, trace message!");
        AutoLog.DEBUG.log("Hello, debug message!");
        AutoLog.WARN.log("Hello, warn message!");
        AutoLog.ERROR.log("Hello, error message!");
        AutoLog.FATAL.log("Hello, fatal message!");
        AutoLog.FATAL.exception(new Exception("Test Exception")).log("Hello, fatal exception message!");

        /*
        Logger2.setIdentities("test", "test", "test");
        Logger2.setLogLevel("test", "test", "debug");
        AutoLog.debug("Hello, %s", "world").action("print").record();

        Logger2.setLogLevel("test", "test", "info");
        AutoLog.debug("Hello, %s", "world").action("print").context("visible=false");
        AutoLog.info("Hello, %s", "world").action("print").context("visible=true, level=INFO");
        AutoLog.warn("This is a simple message").record();
        AutoLog.error(new IllegalAccessException("Sample exception"), "This is a simple log with exception").record();

        // performance tests
        Logger2.setLogLevel("test", "test", "info");
        {
            long start = System.currentTimeMillis();
            for(int i = 0; i < 100000; i++) {
                LogMsg.debug("This is a test message with some formating", String.format("idx=%d, start=%d, message=%s", i, start, "message" + i));
            }
            long end = System.currentTimeMillis();

            System.out.println(String.format("Total elapsed %dms with LogMsg", end - start));
        }

        Logger2.setLogLevel("test", "test", "info");
        {
            long start = System.currentTimeMillis();
            for(int i = 0; i < 100000; i++) {
                if(Logger2.enable(Logger2.DEBUG)) {
                    LogMsg.debug("This is a test message with some formating", String.format("idx=%d, start=%d, message=%s", i, start, "message" + i));
                }
            }
            long end = System.currentTimeMillis();

            System.out.println(String.format("Total elapsed %dms with LogMsg by Logger2.enable", end - start));
        }

        Logger2.setLogLevel("test", "test", "info");
        {
            long start = System.currentTimeMillis();
            for(int i = 0; i < 100000; i++) {
                AutoLog.debug("This is a test message with some formating").context("idx=%d, start=%d, message=%s", i, start, "message" + i);
            }
            long end = System.currentTimeMillis();

            System.out.println(String.format("Total elapsed %dms with AutoLog", end - start));
        }

        {
            long start = System.currentTimeMillis();
            for(int i = 0; i < 100000; i++) {
                String msg = String.format("This is a test message");
            }
            long end = System.currentTimeMillis();

            System.out.println(String.format("Total elapsed %dms with AutoLog", end - start));
        }
        */

        /**
         * Performance result:
         *
         * Total elapsed 473ms with LogMsg
         * Total elapsed 2ms with LogMsg by Logger2.enable
         * Total elapsed 13ms with AutoLog
         */
    }
}
