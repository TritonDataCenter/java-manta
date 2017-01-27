package com.joyent.manta.client.multipart;

import java.io.OutputStream;
import java.lang.UnsupportedOperationException;
import java.io.IOException;

/**
 * Wraps a series of output streams
 * not thread safe..?
 */
public class MultipartOutputStream extends OutputStream {

    private OutputStream wrapped = null;
    

    public MultipartOutputStream(OutputStream first) {
        this.wrapped = first;
    }


    public void setNext(OutputStream next) {
        wrapped = next;
    }

    @Override
	public void close() {
        // something about underlying streams managing the caller
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush() throws IOException {
        wrapped.flush();
    }

    @Override
    public void	write(byte[] b) throws IOException {
        wrapped.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        wrapped.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
        wrapped.write(b);
    }
}
