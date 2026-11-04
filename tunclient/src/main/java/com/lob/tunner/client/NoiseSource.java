package com.lob.tunner.client;

import com.lob.tunner.logger.AutoLog;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Provides a continuous, cyclic stream of noise/dummy bytes from a file or pseudo-random generator.
 */
public class NoiseSource {
    private final byte[] _data;
    private int _position = 0;

    public NoiseSource(String filePath) {
        byte[] loaded = null;
        if (filePath != null && !filePath.trim().isEmpty()) {
            File file = new File(filePath);
            if (file.exists() && file.isFile() && file.length() > 0) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    // Cache up to 16MB into memory for fast cyclic reads
                    int len = (int) Math.min(file.length(), 16 * 1024 * 1024);
                    loaded = new byte[len];
                    int totalRead = 0;
                    while (totalRead < len) {
                        int r = fis.read(loaded, totalRead, len - totalRead);
                        if (r < 0) {
                            break;
                        }
                        totalRead += r;
                    }
                    if (totalRead < len) {
                        byte[] exact = new byte[totalRead];
                        System.arraycopy(loaded, 0, exact, 0, totalRead);
                        loaded = exact;
                    }
                    AutoLog.INFO.log("Loaded %d bytes from noise file: %s", loaded.length, file.getAbsolutePath());
                } catch (Exception e) {
                    AutoLog.ERROR.exception(e).log("Failed to load noise file: %s, falling back to random generator", filePath);
                }
            } else {
                AutoLog.WARN.log("Noise file %s not found or empty, falling back to random generator", filePath);
            }
        }

        if (loaded == null || loaded.length == 0) {
            // Generate a 64KB pseudo-random pattern
            loaded = new byte[65536];
            new Random(System.currentTimeMillis()).nextBytes(loaded);
            AutoLog.INFO.log("Initialized 64KB random noise pattern.");
        }

        this._data = loaded;
    }

    /**
     * Slices the next count bytes cyclically.
     * @param count number of bytes requested
     * @return ByteBuffer containing the requested bytes
     */
    public synchronized ByteBuffer getBytes(int count) {
        ByteBuffer buf = ByteBuffer.allocate(count);
        int remaining = count;
        while (remaining > 0) {
            int available = _data.length - _position;
            int toCopy = Math.min(remaining, available);
            buf.put(_data, _position, toCopy);
            _position = (_position + toCopy) % _data.length;
            remaining -= toCopy;
        }
        buf.rewind();
        return buf;
    }

    public int sourceLength() {
        return _data.length;
    }
}

