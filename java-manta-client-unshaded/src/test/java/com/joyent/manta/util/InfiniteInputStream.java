package com.joyent.manta.util;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.io.InputStream;

public class InfiniteInputStream extends InputStream {

    private final byte[] buffer;

    private int count;

    public InfiniteInputStream(final byte[] buffer) {
        this.buffer = buffer;
        this.count = 0;
    }

    @Override
    public int read() throws IOException {
        count++;
        return buffer[count % buffer.length];
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Validate.notNull(b);
        Validate.isTrue(0 <= off
                && len <= b.length
                && off < b.length
                && off + len <= b.length);

        final int initialCount = count;
        for (int i = off; i < len; i++) {
            count++;
            b[i] = buffer[i % buffer.length];
        }

        return count - initialCount;
    }

}
