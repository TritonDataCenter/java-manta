package com.joyent.manta.client.multipart;

import java.io.OutputStream;
import java.lang.UnsupportedOperationException;
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
    public void	write(byte[] b) throws IOException {
        int outstanding = buf.size() + b.length;
        if (outstanding < blockSize) {
            buf.write(b);
        } else {
            int remainder = outstanding % blockSize;
            flushBuffer();
            wrapped.write(b, 0, b.length-remainder);
            buf.write(b, b.length-remainder, remainder);
        }
    }

    public void flushBuffer() throws IOException {
        wrapped.write(buf.toByteArray());
        buf.reset();
    }

    // how to write reminader...
    public void unbufferedWrite(byte[] b) throws IOException {
        wrapped.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        write(Arrays.copyOfRange(b, off, off+len));
    }

    @Override
    public void write(int b) throws IOException {
        wrapped.write(new byte[] {(byte)b});
    }

}
