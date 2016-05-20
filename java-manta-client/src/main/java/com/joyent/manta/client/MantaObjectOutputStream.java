/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpContent;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
 * as a data source. This implementation uses another thread to keep the Google HTTP
 * Client's {@link OutputStream} open in order to proxy all calls to it. This is far
 * from an ideal implementation. Please only use this class as a last resort when needing
 * to provide compatibility with inflexible APIs that require an {@link OutputStream}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class MantaObjectOutputStream extends OutputStream {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaObjectOutputStream.class);

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
     * are daemon threads.
     */
    public static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(THREAD_FACTORY);

    /**
     * Inner class that provides visibility into the {@link HttpContent} object being
     * put to the server. This allows us to proxy all of the {@link OutputStream} API
     * calls.
     */
    private class EmbeddedHttpContent implements HttpContent {
        /**
         * The actual output stream that is used by Google HTTP Client.
         */
        private volatile OutputStream writer;

        @Override
        public long getLength() throws IOException {
            return -1L;
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
    private final String contentType;

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
                            final String contentType) {
        this.path = path;
        this.httpHelper = httpHelper;
        this.headers = headers;
        this.metadata = metadata;
        this.contentType = contentType;
        this.httpContent = new EmbeddedHttpContent();
        this.completed = EXECUTOR.submit(upload);

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

        try {
            this.objectResponse = this.completed.get();
            this.objectResponse.setContentLength(bytesWritten);
            this.objectResponse.setContentType(contentType);
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
