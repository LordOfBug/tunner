package com.lob.tunner.server;

import com.lob.tunner.client.TunnelClient;
import org.junit.Test;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class EndToEndIntegrationTest {

    @Test
    public void testEndToEndWithNoise() throws Exception {
        // Create a dummy noise file
        File noiseFile = File.createTempFile("test_noise_", ".dat");
        noiseFile.deleteOnExit();
        try (FileOutputStream fos = new FileOutputStream(noiseFile)) {
            byte[] pattern = "NOISE_PATTERN_DATA_1234567890\n".getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 1000; i++) {
                fos.write(pattern);
            }
        }

        int serverPort = 19100;
        int echoPort = 19101;
        int clientListenPort = 19102;

        // 1. Start TunServer in test mode (which starts EchoServer on echoPort)
        Thread serverThread = new Thread(() -> {
            try {
                TunnelServer.main(new String[]{
                        "-m", "test",
                        "-l", String.valueOf(serverPort),
                        "-f", String.valueOf(echoPort)
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        Thread.sleep(800); // Give server a moment to bind

        // 2. Start TunClient with noise enabled (50k rate)
        Thread clientThread = new Thread(() -> {
            TunnelClient.main(new String[]{
                    "-i", "127.0.0.1",
                    "-l", String.valueOf(clientListenPort),
                    "-r", String.valueOf(serverPort),
                    "-n", "50k",
                    "--noise-file", noiseFile.getAbsolutePath(),
                    "127.0.0.1"
            });
        });
        clientThread.setDaemon(true);
        clientThread.start();

        Thread.sleep(800); // Give client a moment to bind

        // 3. Connect client application to TunClient
        try (Socket socket = new Socket("127.0.0.1", clientListenPort)) {
            socket.setSoTimeout(5000);
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            // Test small message
            String ping = "Ping over multiplexed tunnel!\n";
            out.write(ping.getBytes(StandardCharsets.UTF_8));
            out.flush();

            byte[] buf = new byte[ping.length()];
            int read = in.read(buf);
            assertEquals(ping.length(), read);
            assertEquals(ping, new String(buf, StandardCharsets.UTF_8));

            // Sleep to let noise generator stream dummy data in background
            Thread.sleep(500);

            // Test larger payload transfer (e.g. 10KB)
            byte[] largePayload = new byte[10240];
            for (int i = 0; i < largePayload.length; i++) {
                largePayload[i] = (byte) ('a' + (i % 26));
            }
            out.write(largePayload);
            out.flush();

            byte[] receivedPayload = new byte[largePayload.length];
            int total = 0;
            while (total < largePayload.length) {
                int r = in.read(receivedPayload, total, largePayload.length - total);
                assertTrue(r > 0);
                total += r;
            }
            assertArrayEquals(largePayload, receivedPayload);
        }
    }
}

