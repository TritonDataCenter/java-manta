package com.joyent.manta;

import org.apache.commons.lang3.RandomUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Elijah Zupancic
 * @since 1.0.0
 */
public class RandomInputStream extends InputStream {
    private static final int EOF = -1;
    private final long maximumBytes;
    private volatile long count;

    public RandomInputStream(final long maximumBytes) {
        this.maximumBytes = maximumBytes;
    }

    @Override
    public int read() throws IOException {
        if (count > maximumBytes) {
            return EOF;
        }
        count++;

        return RandomUtils.nextInt(0, Integer.MAX_VALUE);
    }
}
