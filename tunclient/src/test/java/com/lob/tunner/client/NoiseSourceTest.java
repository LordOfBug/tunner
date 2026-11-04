package com.lob.tunner.client;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class NoiseSourceTest {
    @Test
    public void testRandomNoiseFallback() {
        NoiseSource source = new NoiseSource(null);
        assertEquals(65536, source.sourceLength());

        ByteBuffer buf1 = source.getBytes(100);
        assertEquals(100, buf1.remaining());

        ByteBuffer buf2 = source.getBytes(70000); // Exceeds capacity, should wrap around
        assertEquals(70000, buf2.remaining());
    }

    @Test
    public void testFileNoiseCyclicRead() throws Exception {
        File tempFile = File.createTempFile("dummy_noise", ".bin");
        tempFile.deleteOnExit();

        byte[] pattern = "ABCDEFGHIJ".getBytes(); // 10 bytes
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            fos.write(pattern);
        }

        NoiseSource source = new NoiseSource(tempFile.getAbsolutePath());
        assertEquals(10, source.sourceLength());

        ByteBuffer buf = source.getBytes(25);
        assertEquals(25, buf.remaining());

        byte[] readBytes = new byte[25];
        buf.get(readBytes);
        String result = new String(readBytes);
        assertEquals("ABCDEFGHIJABCDEFGHIJABCDE", result);
    }
}

