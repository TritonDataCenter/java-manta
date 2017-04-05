/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A read-only {@link SeekableByteChannel} implementation that utilizes
 * the HTTP Range header to allow you to seek any position in an object on
 * Manta. Connection opening to the remote server happens lazily upon the
 * first read() or size() method invoked.
 *
 * @author Elijah Zupancic
 */
public class MantaSeekableByteChannel extends InputStream
        implements SeekableByteChannel {
    /**
     * Constant representing the value returned when we have reached the
     * end of a stream.
     */
    private static final int EOF = -1;

    /**
     * Constant representing an unknown HTTP content length value.
     */
    private static final long UNKNOWN_CONTENT_LENGTH = -1L;

    /**
     * Flag indicating if the channel is open. Marked as volatile so
     * that different threads can flip its state.
     */
    private volatile boolean open = true;

    /**
     * Path of the object on the Manta API.
     */
    private final String path;

    /**
     * Current position in bytes from the start of the file.
     */
    private AtomicLong position = new AtomicLong(0L);

    /**
     * Helper class providing useful HTTP functions.
     */
    private final HttpHelper httpHelper;

    /**
     * Threadsafe reference to the Manta API HTTP request object.
     */
    private final AtomicReference<HttpUriRequest> requestRef;

    /**
     * Threadsafe reference to the Manta API HTTP response object.
     */
    private final AtomicReference<MantaObjectInputStream> responseStream;

    /**
     * Connection factory instance used for building requests to Manta.
     */
    private final MantaConnectionFactory connectionFactory;

    /**
     * Creates a new instance of a read-only seekable byte channel.
     *
     * @param path path of the object on the Manta API
     * @param position starting position in bytes from the start of the file
     * @param connectionFactory connection factory instance used for building requests to Manta
     * @param httpHelper helper class providing useful HTTP functions
     */
    public MantaSeekableByteChannel(final String path,
                                    final long position,
                                    final MantaConnectionFactory connectionFactory,
                                    final HttpHelper httpHelper) {
        this.path = path;
        this.position = new AtomicLong(position);
        this.connectionFactory = connectionFactory;
        this.httpHelper = httpHelper;
        this.requestRef = new AtomicReference<>();
        this.responseStream = new AtomicReference<>();
    }

    /**
     * Creates a new instance of a read-only seekable byte channel.
     *
     * @param path path of the object on the Manta API
     * @param connectionFactory connection factory instance used for building requests to Manta
     * @param httpHelper helper class providing useful HTTP functions
     */
    public MantaSeekableByteChannel(final String path,
                                    final MantaConnectionFactory connectionFactory,
                                    final HttpHelper httpHelper) {
        this(path, 0L, connectionFactory, httpHelper);
    }

    /**
     * Constructor used for creating a new instance of a read-only seekable byte
     * channel from within this class. This is used when position() is called.
     *
     * @param requestRef reference to existing HTTP request
     * @param responseStream reference to existing HTTP response
     * @param path path of the object on the Manta API
     * @param position starting position in bytes from the start of the file
     * @param connectionFactory connection factory instance used for building requests to Manta
     * @param httpHelper helper class providing useful HTTP functions
     */
    protected MantaSeekableByteChannel(final AtomicReference<HttpUriRequest> requestRef,
                                       final AtomicReference<MantaObjectInputStream> responseStream,
                                       final String path,
                                       final AtomicLong position,
                                       final MantaConnectionFactory connectionFactory,
                                       final HttpHelper httpHelper) {
        this.requestRef = requestRef;
        this.responseStream = responseStream;
        this.path = path;
        this.position = position;
        this.connectionFactory = connectionFactory;
        this.httpHelper = httpHelper;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();
        final long size = size();

        if (position.get() >= size) {
            return EOF;
        }

        final byte[] buff = dst.array();
        final int bytesRead = stream.read(buff);

        position.addAndGet(bytesRead);

        return bytesRead;
    }

    /**
     * Reads the next byte of data from the backing input stream at the current
     * position. The value byte is returned as an <code>int</code> in the range
     * <code>0</code> to <code>255</code>. If no byte is available because the
     * end of the stream has been reached, the value <code>-1</code> is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     *
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             stream is reached.
     * @exception  IOException  if an I/O error occurs.
     */
    @Override
    public int read() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();
        final int read = stream.read();

        if (read > -1) {
            position.incrementAndGet();
        }

        return read;
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();

        final int totalRead = stream.read(buffer);

        if (totalRead > -1) {
            position.addAndGet(totalRead);
        }

        return totalRead;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length)
            throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();

        final int totalRead = stream.read(buffer, offset, length);

        if (totalRead > -1) {
            position.addAndGet(totalRead);
        }

        return totalRead;
    }

    @Override
    public long skip(final long noOfBytesToSkip) throws IOException {
        if (!open) {
            return 0;
        }

        final MantaObjectInputStream stream = connectOrGetResponse();
        final long totalSkipped = stream.skip(noOfBytesToSkip);

        position.addAndGet(totalSkipped);

        return totalSkipped;
    }

    @Override
    public int available() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();

        return stream.available();
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        // This is a read-only channel
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        return position.get();
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
        return new MantaSeekableByteChannel(
                new AtomicReference<>(),
                new AtomicReference<>(),
                path,
                new AtomicLong(newPosition),
                connectionFactory,
                httpHelper);
    }

    @Override
    public long size() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final MantaObjectInputStream stream = connectOrGetResponse();
        final long contentLength = stream.getContentLength();

        if (contentLength == UNKNOWN_CONTENT_LENGTH) {
            MantaClientException e = new MantaClientException(
                    "Can't get SeekableByteChannel for objects of unknown size");
            HttpUriRequest request = requestRef.get();
            if (request != null) {
                @SuppressWarnings("unchecked")
                HttpResponse response = (HttpResponse)stream.getHttpResponse();
                HttpHelper.annotateContextedException(e, request, response);
            }

            throw e;
        }

        return contentLength;
    }

    @Override
    public synchronized void mark(final int readlimit) {
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public SeekableByteChannel truncate(final long newSize) throws IOException {
        throw new NonWritableChannelException();
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public synchronized void close() throws IOException {
        if (!open) {
            return;
        }

        MantaObjectInputStream stream = responseStream.getAndSet(null);
        IOUtils.closeQuietly(stream);

        open = false;
    }

    /**
     * Connects to the Manta API, updates the atomic reference and returns a
     * response if the atomic reference hasn't been set. Otherwise, it just returns
     * the response embedded in the atomic reference.
     *
     * @return HTTP response object
     * @throws IOException thrown when there are network problems connecting to the remote API
     */
    protected synchronized MantaObjectInputStream connectOrGetResponse() throws IOException {
        if (responseStream.get() != null) {
            return responseStream.get();
        }

        final HttpUriRequest request = connectionFactory.get(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        // Set byte range requested via HTTP range header
        headers.setRange(String.format("bytes=%d-", position.get()));

        // Store the request so that we can use it for adding information to exceptions
        this.requestRef.set(request);

        MantaObjectInputStream stream = httpHelper.httpRequestAsInputStream(
                request, headers);
        responseStream.compareAndSet(null, stream);

        final String contentType = stream.getContentType();

        if (MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE.equals(contentType)) {
            MantaClientException e = new MantaClientException(
                    "Can't get SeekableByteChannel for directory objects");

            @SuppressWarnings("unchecked")
            HttpResponse response = (HttpResponse)stream.getHttpResponse();
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        return stream;
    }
}
