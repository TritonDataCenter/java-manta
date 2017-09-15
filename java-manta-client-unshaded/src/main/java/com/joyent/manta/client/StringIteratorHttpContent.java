/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Implementation of {@link HttpEntity} that allows for the real-time streaming
 * of data from an iterator or a Java 8 stream to an {@link OutputStream} that
 * is connected to HTTP content.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StringIteratorHttpContent implements HttpEntity {
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
    private final ContentType contentType;

    /**
     * Total bytes of content. Defaults to -1 before content is written.
     */
    private AtomicLong length = new AtomicLong(-1L);


    /**
     * Create a new instance based on a {@link Iterator} of strings.
     *
     * @param iterator iterator of strings for each line
     * @param contentType content (mime) type associated with content
     */
    public StringIteratorHttpContent(final Iterator<String> iterator,
                                     final ContentType contentType) {
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
                                     final ContentType contentType) {
        this.stream = stream;
        this.iterator = null;
        this.contentType = contentType;
    }

    @Override
    public boolean isRepeatable() {
        return false;
    }

    @Override
    public boolean isChunked() {
        return false;
    }

    @Override
    public long getContentLength() {
        return length.get();
    }

    @Override
    public Header getContentType() {
        return new BasicHeader(HttpHeaders.CONTENT_TYPE, contentType.toString());
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

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
        throw new UnsupportedOperationException("getContent isn't supported");
    }

    @Override
    @Deprecated
    public void consumeContent() throws IOException {
        throw new UnsupportedOperationException("getContent isn't supported");
    }

    @Override
    public Header getContentEncoding() {
        // the content encoding is unknown, so we return null
        return null;
    }

    @Override
    public boolean isStreaming() {
        return true;
    }

    /**
     * Write all of the strings in the stored iterator to the passed
     * {@link OutputStream}.
     *
     * @param out output to write to
     * @throws IOException thrown when we can't write
     */
    protected void writeIterator(final OutputStream out) throws IOException {
        Validate.notNull(iterator, "Iterator must not be null");

        // Start length at zero because it is set to -1 by default
        length.set(0L);

        while (iterator.hasNext()) {
            String next = iterator.next();
            if (next == null) {
                continue;
            }

            // We use Unix new lines intentionally for the Manta service
            final String formatted = String.format("%s%s", next, StringUtils.LF);
            byte[] bytes = formatted.getBytes(StandardCharsets.UTF_8);
            length.addAndGet(bytes.length);
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
        Validate.notNull(stream, "Stream must not be null");

        // Start length at zero because it is set to -1 by default
        length.set(0L);

        /* This horribly contorted exception handling is because Java 8
         * streams do not support checked exception handling. */
        try {
            stream.forEach(item -> {
                if (item != null) {
                    try {
                        String formatted = String.format("%s\n", item);
                        byte[] bytes = formatted.getBytes(StandardCharsets.UTF_8);
                        length.addAndGet(bytes.length);
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
        private static final long serialVersionUID = 1317022643615834337L;

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
