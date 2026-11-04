package com.lob.tunner.common;

import org.junit.Test;
import static org.junit.Assert.*;

public class ConfigTest {
    @Test
    public void testParseRate() {
        assertEquals(0L, Config.parseRate("0"));
        assertEquals(0L, Config.parseRate(""));
        assertEquals(0L, Config.parseRate(null));
        assertEquals(1024L, Config.parseRate("1k"));
        assertEquals(10 * 1024L, Config.parseRate("10k"));
        assertEquals(10 * 1024L, Config.parseRate("10kb"));
        assertEquals(1024L * 1024L, Config.parseRate("1m"));
        assertEquals(500 * 1024L * 1024L, Config.parseRate("500m"));
        assertEquals(2L * 1024L * 1024L * 1024L, Config.parseRate("2g"));
    }
}

