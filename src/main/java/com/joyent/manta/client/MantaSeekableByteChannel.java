package com.joyent.manta.client;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.joyent.manta.exception.MantaClientException;

import javax.annotation.concurrent.ThreadSafe;
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
@ThreadSafe
public class MantaSeekableByteChannel implements SeekableByteChannel {
    private static final int EOF = -1;

    private volatile boolean open = true;
    private final GenericUrl objectUri;
    private volatile long position = 0L;
    private final HttpRequestFactory httpRequestFactory;
    private final AtomicReference<HttpResponse> responseRef;

    public MantaSeekableByteChannel(final GenericUrl objectUri,
                                    final long position,
                                    final HttpRequestFactory httpRequestFactory) {
        this.objectUri = objectUri;
        this.position = position;
        this.httpRequestFactory = httpRequestFactory;
        this.responseRef = new AtomicReference<>();

    }

    public MantaSeekableByteChannel(final GenericUrl objectUri,
                                    final HttpRequestFactory httpRequestFactory) {
        this(objectUri, 0L, httpRequestFactory);
    }

    protected MantaSeekableByteChannel(final AtomicReference<HttpResponse> responseRef,
                                       final GenericUrl objectUri,
                                       final long position,
                                       final HttpRequestFactory httpRequestFactory) {
        this.responseRef = responseRef;
        this.objectUri = objectUri;
        this.position = position;
        this.httpRequestFactory = httpRequestFactory;
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final long size = size();

        if (position >= size) {
            return EOF;
        }

        final InputStream is = response.getContent();
        final byte[] buff = dst.array();
        final int bytesRead = is.read(buff);

        position += bytesRead;

        return bytesRead;
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
        return new MantaSeekableByteChannel(new AtomicReference<HttpResponse>(),
                objectUri, newPosition, httpRequestFactory);
    }

    @Override
    public long size() throws IOException {
        if (!open) {
            throw new ClosedChannelException();
        }

        final HttpResponse response = connectOrGetResponse();
        final HttpHeaders headers = response.getHeaders();

        if (headers.getContentLength() == null) {
            throw new MantaClientException("Can't get SeekableByteChannel for objects of unknown size");
        }

        return headers.getContentLength();
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

        if (responseRef.get() != null) {
            responseRef.get().disconnect();
        }

        open = false;
    }


    protected HttpResponse connectOrGetResponse() throws IOException {
        if (responseRef.get() != null) {
            return responseRef.get();
        }

        final HttpRequest request = httpRequestFactory.buildGetRequest(objectUri);
        final HttpHeaders headers = request.getHeaders();

        headers.setRange(String.format("bytes=%d-", position));

        responseRef.compareAndSet(null, request.execute());
        final HttpResponse response = responseRef.get();
        final String contentType = response.getHeaders().getContentType();

        if (MantaObjectMetadata.DIRECTORY_RESPONSE_CONTENT_TYPE.equals(contentType)) {
            throw new MantaClientException("Can't get SeekableByteChannel for directory objects");
        }

        return response;
    }
}
