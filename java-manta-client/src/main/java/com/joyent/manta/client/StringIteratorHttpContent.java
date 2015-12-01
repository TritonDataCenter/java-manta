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
 * Implementation of {@link HttpContent} that allows for the real-time streaming
 * of data from an iterator or a Java 8 stream to an {@link OutputStream} that
 * is connected to HTTP content.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StringIteratorHttpContent implements HttpContent {
    /**
     * Iterator containing lines to stream into content.
     */
    private final Iterator<String> iterator;

    /**
     * Java 8 stream containing lines to stream into content.
     */
    private final Stream<String> stream;

    /**
     * Content (mime) type associated with content.
     */
    private final String contentType;

    /**
     * Total bytes of content. Defaults to -1 before content is written.
     */
    private volatile long length = -1L;


    /**
     * Create a new instance based on a {@link Iterator} of strings.
     *
     * @param iterator iterator of strings for each line
     * @param contentType content (mime) type associated with content
     */
    public StringIteratorHttpContent(final Iterator<String> iterator,
                                     final String contentType) {
        this.iterator = iterator;
        this.stream = null;
        this.contentType = contentType;
    }


    /**
     * Create a new instance based on a {@link Stream} of strings.
     * Stream will be closed after all elements are read.
     *
     * @param stream stream of strings for each line
     * @param contentType content (mime) type associated with content
     */
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
    public void writeTo(final OutputStream out) throws IOException {
        try {
            if (iterator != null) {
                writeIterator(out);
            } else if (stream != null) {
                writeStream(out);
            }
        } finally {
            out.close();

            if (stream != null) {
                stream.close();
            }
        }
    }


    /**
     * Write all of the strings in the stored iterator to the passed
     * {@link OutputStream}.
     *
     * @param out output to write to
     * @throws IOException thrown when we can't write
     */
    protected void writeIterator(final OutputStream out) throws IOException {
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


    /**
     * Write all of the strings in the stored stream to the passed
     * {@link OutputStream}.
     *
     * @param out output to write to
     * @throws IOException thrown when we can't write
     */
    protected void writeStream(final OutputStream out) throws IOException {
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


    /**
     * Inner exception class for handling the wrapping of {@link IOException}
     * when processing Java 8 streams.
     */
    protected static class StreamIOException extends RuntimeException {
        /**
         * Create an instance that wraps the passed cause.
         *
         * @param cause {@link IOException} to wrap
         */
        public StreamIOException(final Throwable cause) {
            super(cause);
        }

        /**
         * @return the wrapped exception as a {@link IOException}
         */
        @SuppressWarnings("unchecked")
        IOException getIOCause() {
            return (IOException)getCause();
        }
    }
}
