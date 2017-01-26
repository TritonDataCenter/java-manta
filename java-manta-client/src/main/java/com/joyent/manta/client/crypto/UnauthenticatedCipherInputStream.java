package com.joyent.manta.client.crypto;

import org.apache.commons.io.input.CountingInputStream;
import org.bouncycastle.jcajce.io.CipherInputStream;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.InputStream;

public class UnauthenticatedCipherInputStream extends CipherInputStream {
    private static final int EOF = -1;
    private final long contentLength;
    private volatile boolean endOfStream = false;
    private final CountingInputStream backingStream;

    public UnauthenticatedCipherInputStream(final InputStream input,
                                            final Cipher cipher,
                                            final long contentLength,
                                            final CountingInputStream backingStream) {
        super(input, cipher);
        this.contentLength = contentLength;
        this.backingStream = backingStream;
    }

    @Override
    public int read() throws IOException {
        if (endOfStream) {
            return EOF;
        }

        if (backingStream.getByteCount() >= contentLength) {
            endOfStream = true;
            return EOF;
        }

        return super.read();
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length) throws IOException {
        if (endOfStream) {
            return EOF;
        }

        if (backingStream.getByteCount() >= contentLength) {
            endOfStream = true;
            return EOF;
        }

        final int adjustedLength;
        final long difference = contentLength - backingStream.getByteCount();

        if (difference < length) {
            adjustedLength = (int)difference;
        } else {
            adjustedLength = length;
        }

        return super.read(buffer, offset, adjustedLength);
    }

    @Override
    public long skip(final long numberOfBytesToSkip) throws IOException {
        if (endOfStream) {
            return EOF;
        }

        if (backingStream.getByteCount() >= contentLength) {
            endOfStream = true;
            return EOF;
        }

        final long adjustedLength;
        final long difference = contentLength - backingStream.getByteCount();

        if (difference < numberOfBytesToSkip) {
            adjustedLength = difference;
        } else {
            adjustedLength = numberOfBytesToSkip;
        }

        return super.skip(adjustedLength);
    }
}
