package com.joyent.manta.util;

import org.apache.commons.lang3.Validate;

import javax.crypto.Mac;
import java.io.IOException;
import java.io.InputStream;

/**
 * {@link InputStream} implementation that progressively updates a
 * HMAC as data is read.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class HmacInputStream extends InputStream {
    /**
     * HMAC instance used to generate HMAC for wrapped stream.
     */
    private final Mac hmac;

    /**
     * Underlying stream being wrapped.
     */
    private final InputStream chained;

    /**
     * Creates a new instance using the specified HMAC instance and wrapping
     * the specified stream.
     *
     * @param hmac HMAC instance that has been initialized
     * @param chained stream being wrapped
     */
    public HmacInputStream(final Mac hmac, final InputStream chained) {
        Validate.notNull(hmac, "HMAC instance must not be null");
        Validate.notNull(chained, "InputStream must not be null");

        this.hmac = hmac;
        this.chained = chained;
    }

    public Mac getHmac() {
        return hmac;
    }

    @Override
    public int read() throws IOException {
        final int read = chained.read();
        hmac.update((byte)read);

        return read;
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        final int read = chained.read(buffer);

        if (read > -1) {
            hmac.update(buffer, 0, read);
        }

        return read;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        final int read = chained.read(buffer);
        hmac.update(buffer, offset, read);

        return read;
    }

    @Override
    public long skip(final long numberOfBytesToSkip) throws IOException {
        long bytesRead = 0;

        for (long l = 0; l < numberOfBytesToSkip; l++) {
            final int read = read();

            if (read > -1) {
                bytesRead++;
            } else {
                break;
            }
        }

        return bytesRead;
    }

    @Override
    public int available() throws IOException {
        return chained.available();
    }

    @Override
    public void close() throws IOException {
        chained.close();
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void mark(final int readlimit) {
        throw new UnsupportedOperationException("mark is not a supported operation on " + getClass());
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException("reset is not a supported operation on " + getClass());
    }
}
