/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpContent;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StringIteratorHttpContent implements HttpContent {
    private final Iterator<String> iterator;
    private final Stream<String> stream;
    private final String contentType;
    private volatile long length = -1L;


    public StringIteratorHttpContent(final Iterator<String> iterator,
                                     final String contentType) {
        this.iterator = iterator;
        this.stream = null;
        this.contentType = contentType;
    }


    public StringIteratorHttpContent(final Stream<String> stream,
                                     final String contentType) {
        this.stream = stream;
        this.iterator = null;
        this.contentType = contentType;
    }


    @Override
    public long getLength() throws IOException {
        return length;
    }


    @Override
    public String getType() {
        return contentType;
    }


    @Override
    public boolean retrySupported() {
        return false;
    }


    @Override
    public void writeTo(OutputStream out) throws IOException {
        try {
            if (iterator != null) {
                writeIterator(out);
            } else if (stream != null) {
                writeStream(out);
            }
        } finally {
            out.close();
        }
    }


    protected void writeIterator(OutputStream out) throws IOException {
        Objects.requireNonNull(iterator, "Iterator must be present");

        // Start length at zero because it is set to -1 by default
        length = 0L;

        while (iterator.hasNext()) {
            String next = iterator.next();
            if (next == null) {
                continue;
            }

            String formatted = String.format("%s\n", next);
            byte[] bytes = formatted.getBytes("UTF-8");
            length += bytes.length;
            out.write(bytes);
        }
    }


    protected void writeStream(OutputStream out) throws IOException {
        Objects.requireNonNull(stream, "Stream must be present");

        // Start length at zero because it is set to -1 by default
        length = 0L;

        /* This horribly contorted exception handling is because Java 8
         * streams do not support checked exception handling. */
        try {
            stream.forEach(item -> {
                if (item != null) {
                    try {
                        String formatted = String.format("%s\n", item);
                        byte[] bytes = formatted.getBytes("UTF-8");
                        length += bytes.length;
                        out.write(bytes);
                    } catch (IOException e) {
                        throw new StreamIOException(e);
                    }
                }
            });
        } catch (StreamIOException e) {
            throw e.getIOCause();
        }
    }


    protected static class StreamIOException extends RuntimeException {
        public StreamIOException(Throwable cause) {
            super(cause);
        }

        @SuppressWarnings("unchecked")
        IOException getIOCause() {
            return (IOException)getCause();
        }
    }
}
