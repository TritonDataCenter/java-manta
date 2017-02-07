package com.joyent.manta.client.multipart;

import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/**
 * Wraps a series of output streams
 * not thread safe..?
 * performance is terrible
 */
public class MultipartOutputStream extends OutputStream {

    private OutputStream wrapped = null;
    private ByteArrayOutputStream buf;
    private int blockSize;

    public MultipartOutputStream(int blockSize) {
        this.blockSize = blockSize;
        buf = new ByteArrayOutputStream(blockSize);
    }


    public void setNext(OutputStream next) {
        wrapped = next;
    }


    // caller must check this before closing everything out
    public byte[] getRemainder() {
        return buf.toByteArray();
    }


    // DOES NOT CLOSE UNDERLYING STREAM
    // It might look reasonable to flush the buffer here.  But that
    // would complicate HMAC where we need to writ the final "normal
    // bytes", flush the buffer, then write the HMAC, and finally
    // close the underlying buffers up.
    @Override
	public void close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException {
        wrapped.flush();
    }

    @Override
    public void	write(final byte[] buffer) throws IOException {
        int outstanding = buf.size() + buffer.length;
        if (outstanding < blockSize) {
            buf.write(buffer);
        } else {
            int remainder = outstanding % blockSize;
            flushBuffer();
            wrapped.write(buffer, 0, buffer.length - remainder);
            buf.write(buffer, buffer.length - remainder, remainder);
        }
    }

    public void flushBuffer() throws IOException {
        wrapped.write(buf.toByteArray());
        buf.reset();
    }

    @Override
    public void write(final byte[] buffer, final int offset, final int length) throws IOException {
        write(Arrays.copyOfRange(buffer, offset, offset + length));
    }

    @Override
    public void write(final int value) throws IOException {
        wrapped.write(value);
    }
}
