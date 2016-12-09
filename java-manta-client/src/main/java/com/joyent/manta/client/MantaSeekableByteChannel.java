package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.http.MantaConnectionFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.SeekableByteChannel;
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
    private volatile long position = 0L;

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
    private final AtomicReference<CloseableHttpResponse> responseRef;

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
        this.position = position;
        this.connectionFactory = connectionFactory;
        this.httpHelper = httpHelper;
        this.requestRef = new AtomicReference<>();
        this.responseRef = new AtomicReference<>();
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
     * @param responseRef reference to existing HTTP response
     * @param path path of the object on the Manta API
     * @param position starting position in bytes from the start of the file
     * @param connectionFactory connection factory instance used for building requests to Manta
     * @param httpHelper helper class providing useful HTTP functions
     */
    protected MantaSeekableByteChannel(final AtomicReference<HttpUriRequest> requestRef,
                                       final AtomicReference<CloseableHttpResponse> responseRef,
                                       final String path,
                                       final long position,
                                       final MantaConnectionFactory connectionFactory,
                                       final HttpHelper httpHelper) {
        this.requestRef = requestRef;
        this.responseRef = responseRef;
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

        final CloseableHttpResponse response = connectOrGetResponse();
        final long size = size();

        if (position >= size) {
            return EOF;
        }

        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();
        final byte[] buff = dst.array();
        final int bytesRead = is.read(buff);

        position += bytesRead;

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

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();
        position++;
        return is.read();
    }

    @Override
    public int read(final byte[] buffer) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();

        final int totalRead = is.read(buffer);

        if (totalRead > -1) {
            position += totalRead;
        }

        return totalRead;
    }

    @Override
    public int read(final byte[] buffer, final int offset, final int length)
            throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();

        final int totalRead = is.read(buffer, offset, length);

        if (totalRead > -1) {
            position += totalRead;
        }

        return totalRead;
    }

    @Override
    public long skip(final long noOfBytesToSkip) throws IOException {
        if (!open) {
            return 0;
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();

        final long totalSkipped = is.skip(noOfBytesToSkip);

        position += totalSkipped;

        return totalSkipped;
    }



    @Override
    public int available() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final InputStream is = entity.getContent();

        return is.available();
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
        // This is a read-only channel
        throw new NonWritableChannelException();
    }

    @Override
    public long position() throws IOException {
        return position;
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
        return new MantaSeekableByteChannel(
                new AtomicReference<>(),
                new AtomicReference<>(),
                path,
                newPosition,
                connectionFactory,
                httpHelper);
    }

    @Override
    public long size() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpEntity entity = response.getEntity();
        final long contentLength = entity.getContentLength();

        if (contentLength == UNKNOWN_CONTENT_LENGTH) {
            MantaClientException e = new MantaClientException(
                    "Can't get SeekableByteChannel for objects of unknown size");
            HttpUriRequest request = requestRef.get();
            if (request != null) {
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

        CloseableHttpResponse response = responseRef.getAndSet(null);
        IOUtils.closeQuietly(response);

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
    protected synchronized CloseableHttpResponse connectOrGetResponse() throws IOException {
        if (responseRef.get() != null) {
            return responseRef.get();
        }

        final HttpUriRequest request = connectionFactory.get(path);

        // Set byte range requested via HTTP range header
        request.setHeader(HttpHeaders.RANGE, String.format("bytes=%d-", position));

        // Store the request so that we can use it for adding information to exceptions
        this.requestRef.set(request);

        CloseableHttpResponse response = httpHelper.executeRequest(request,
                "GET    {} response [{}] {} ");
        responseRef.compareAndSet(null, response);

        final HttpEntity entity = response.getEntity();
        final String contentType = entity.getContentType().getValue();

        if (MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE.equals(contentType)) {
            MantaClientException e = new MantaClientException(
                    "Can't get SeekableByteChannel for directory objects");
            HttpHelper.annotateContextedException(e, request, response);
            throw e;
        }

        return response;
    }
}
