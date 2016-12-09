/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link OutputStream} that wraps the PUT operations using an {@link java.io.InputStream}
 * as a data source. This implementation uses another thread to keep the Apache HTTP
 * Client's {@link OutputStream} open in order to proxy all calls to it. This is far
 * from an ideal implementation. Please only use this class as a last resort when needing
 * to provide compatibility with inflexible APIs that require an {@link OutputStream}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class MantaObjectOutputStream extends OutputStream {
    /**
     * Thread group for all Manta output stream threads.
     */
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("manta-outputstream");

    /**
     * Number of milliseconds to wait between checks to see if the stream has been closed.
     */
    private static final long CLOSED_CHECK_INTERVAL = 50L;

    /**
     * Custom thread factory that makes sensibly named daemon threads.
     */
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
        private final AtomicInteger count = new AtomicInteger(1);

        @Override
        public Thread newThread(final Runnable runnable) {
            final String name = String.format("stream-%d", count.getAndIncrement());
            Thread thread = new Thread(THREAD_GROUP, runnable, name);
            thread.setDaemon(true);

            return thread;
        }
    };

    /**
     * Global executor service used for scheduling Manta OutputStream threads.
     * You shouldn't need to call shutdown on this because all of the threads scheduled
     * are daemon threads, but it is exposed so that you can manage its lifecycle
     * if needed.
     */
    public static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(THREAD_FACTORY);

    /**
     * Inner class that provides visibility into the {@link org.apache.http.HttpEntity} object being
     * put to the server. This allows us to proxy all of the {@link OutputStream} API
     * calls.
     */
    private class EmbeddedHttpContent implements HttpEntity {
        /**
         * The actual output stream that is used by Apache HTTP Client.
         */
        private volatile OutputStream writer;

        @Override
        public synchronized void writeTo(final OutputStream out) throws IOException {
            writer = out;

            /* Loop while the parent OutputStream is still open. This allows us to write
             * to the stream from the parent class while keeping the stream open with
             * another thread. */
            while (!isClosed) {
                try {
                    this.wait(CLOSED_CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    return; // exit loop and assume closed if interrupted
                }
            }
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
            return -1L;
        }

        /**
         * Obtains the Content-Type header, if known.
         * This is the header that should be used when sending the entity,
         * or the one that was received with the entity. It can include a
         * charset attribute.
         *
         * @return the Content-Type header for this entity, or
         * {@code null} if the content type is unknown
         */
        @Override
        public Header getContentType() {
            return new BasicHeader(HttpHeaders.CONTENT_TYPE, contentType.toString());
        }

        /**
         * Obtains the Content-Encoding header, if known.
         * This is the header that should be used when sending the entity,
         * or the one that was received with the entity.
         * Wrapping entities that modify the content encoding should
         * adjust this header accordingly.
         *
         * @return the Content-Encoding header for this entity, or
         * {@code null} if the content encoding is unknown
         */
        @Override
        public Header getContentEncoding() {
            return null;
        }

        /**
         * Returns a content stream of the entity.
         * {@link #isRepeatable Repeatable} entities are expected
         * to create a new instance of {@link InputStream} for each invocation
         * of this method and therefore can be consumed multiple times.
         * Entities that are not {@link #isRepeatable repeatable} are expected
         * to return the same {@link InputStream} instance and therefore
         * may not be consumed more than once.
         * <p>
         * IMPORTANT: Please note all entity implementations must ensure that
         * all allocated resources are properly deallocated after
         * the {@link InputStream#close()} method is invoked.
         *
         * @return content stream of the entity.
         * @throws IOException                   if the stream could not be created
         * @throws UnsupportedOperationException if entity content cannot be represented as {@link InputStream}.
         * @see #isRepeatable()
         */
        @Override
        public InputStream getContent() throws IOException, UnsupportedOperationException {
            throw new UnsupportedOperationException("getContent is not supported");
        }

        /**
         * Tells whether this entity depends on an underlying stream.
         * Streamed entities that read data directly from the socket should
         * return {@code true}. Self-contained entities should return
         * {@code false}. Wrapping entities should delegate this call
         * to the wrapped entity.
         *
         * @return {@code true} if the entity content is streamed,
         * {@code false} otherwise
         */
        @Override
        public boolean isStreaming() {
            return false;
        }

        /**
         * This method is deprecated since version 4.1. Please use standard
         * java convention to ensure resource deallocation by calling
         * {@link InputStream#close()} on the input stream returned by
         * {@link #getContent()}
         * <p>
         * This method is called to indicate that the content of this entity
         * is no longer required. All entity implementations are expected to
         * release all allocated resources as a result of this method
         * invocation. Content streaming entities are also expected to
         * dispose of the remaining content, if any. Wrapping entities should
         * delegate this call to the wrapped entity.
         * <p>
         * This method is of particular importance for entities being
         * received from a {@link org.apache.http.client.HttpClient connection}.
         * The entity needs to be consumed completely in order to re-use the
         * connection with keep-alive.
         *
         * @throws IOException if an I/O error occurs.
         * @see #getContent() and #writeTo(OutputStream)
         * @deprecated (4.1) Use {@link EntityUtils#consume(HttpEntity)}
         */
        @Override
        @Deprecated
        public void consumeContent() throws IOException {
            throw new UnsupportedOperationException("consumeContent is not supported");
        }
    }

    /**
     * Thread execution definition that runs the HTTP PUT operation.
     */
    private Callable<MantaObjectResponse> upload = new Callable<MantaObjectResponse>() {
        @Override
        public MantaObjectResponse call() throws Exception {
            return httpHelper.httpPut(path, headers, httpContent, metadata);
        }
    };

    /**
     * Path to the object in Manta.
     */
    private final String path;

    /**
     * The helper object that provides a PUT interface.
     */
    private final HttpHelper httpHelper;

    /**
     * The headers to send along with the PUT request.
     */
    private final MantaHttpHeaders headers;

    /**
     * The metadata to metadata to send along with the PUT request.
     */
    private final MantaMetadata metadata;

    /**
     * Content type value for the file being uploaded.
     */
    private final ContentType contentType;

    /**
     * Http content object that is proxied by this stream.
     */
    private final EmbeddedHttpContent httpContent;

    /**
     * {@link Future} that represents upload thread running.
     */
    private final Future<MantaObjectResponse> completed;

    /**
     * The response object when the upload has finished.
     */
    private MantaObjectResponse objectResponse;

    /**
     * A running count of the total number of bytes written.
     */
    private volatile long bytesWritten = 0L;

    /**
     * Flag indicating that this stream has been closed.
     */
    private volatile boolean isClosed = false;

    /**
     * Creates a new instance of an {@link OutputStream} that wraps PUT
     * requests to Manta.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param httpHelper reference to HTTP operations helper class
     * @param headers optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @param contentType HTTP Content-Type header value
     */
    MantaObjectOutputStream(final String path, final HttpHelper httpHelper,
                            final MantaHttpHeaders headers,
                            final MantaMetadata metadata,
                            final ContentType contentType) {
        this.path = path;
        this.httpHelper = httpHelper;
        this.headers = headers;
        this.metadata = metadata;
        this.contentType = contentType;
        this.httpContent = new EmbeddedHttpContent();
        this.completed = EXECUTOR.submit(upload);

        /**
         * We have to wait here until the upload to Manta starts and a Writer
         * becomes available.
         */
        while (httpContent.writer == null) {
            try {
                Thread.sleep(CLOSED_CHECK_INTERVAL);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override
    public void write(final int b) throws IOException {
        httpContent.writer.write(b);
        bytesWritten++;
    }

    @Override
    public void write(final byte[] b) throws IOException {
        httpContent.writer.write(b);
        bytesWritten += b.length;
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        httpContent.writer.write(b, off, len);
        bytesWritten += b.length;
    }

    @Override
    public void flush() throws IOException {
        httpContent.writer.flush();
    }

    /**
     * Uses reflection to look into the specified {@link OutputStream} instance to
     * see if there is a boolean field set called "closed", if it is set and accessible
     * via reflection, we return its value. Otherwise, we return null.
     *
     * @param stream instance to reflect on for closed property
     * @return reference to closed property or null if unavailable
     */
    protected static Boolean isInnerStreamClosed(final OutputStream stream) {
        try {
            Field f = FieldUtils.getField(stream.getClass(), "closed", true);
            Object result = f.get(stream);
            return  (boolean)result;
        } catch (IllegalArgumentException | IllegalAccessException | ClassCastException e) {
            /* If we don't have an inner field called closed, it is inaccessible or
             * the field isn't a boolean, return null because we are now dealing with
             * undefined behavior. */
            return null;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        Boolean innerIsClosed = isInnerStreamClosed(this.httpContent.writer);
        if (innerIsClosed != null && !innerIsClosed) {
            this.httpContent.writer.flush();
        }

        this.isClosed = true;

        synchronized (this.httpContent) {
            this.httpContent.notify();
        }

        try {
            this.objectResponse = this.completed.get();
            this.objectResponse.setContentLength(bytesWritten);
        } catch (InterruptedException e) {
            // continue execution if interrupted
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }

    /**
     * Flag indicating if the stream has been closed.
     *
     * @return true if closed, otherwise false
     */
    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Returns the PUT response object. This value is only available when
     * close() has completed.
     * @return PUT object response or null
     */
    public MantaObjectResponse getObjectResponse() {
        return objectResponse;
    }
}
