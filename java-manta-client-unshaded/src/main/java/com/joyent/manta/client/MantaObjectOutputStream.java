/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.EmbeddedHttpContent;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.ClosedOutputStream;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <p>{@link OutputStream} that wraps the PUT operations using an {@link java.io.InputStream}
 * as a data source. This implementation uses another thread to keep the Apache HTTP
 * Client's {@link OutputStream} open in order to proxy all calls to it. This is far
 * from an ideal implementation. Please only use this class as a last resort when needing
 * to provide compatibility with inflexible APIs that require an {@link OutputStream}.</p>
 *
 * <p>NOTE: Use of this class should be discouraged because it uses another thread
 * and it uses reflection in order to provide an {@link OutputStream} implementation.
 * It is provided for users who have no choice but to use an {@link OutputStream} to
 * write data to Manta due to constraints in their object model. In terms of
 * actual impact to your application, the only concrete concern is performance
 * and memory utilization.</p>
 *
 * <p>The above limitations of this class are shaped by the APIS provided in the
 * Apache HTTP client library. Perhaps a version a more primitive API could
 * be developed that didn't use another thread nor reflection.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class MantaObjectOutputStream extends OutputStream {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaObjectOutputStream.class);

    /**
     * Thread group for all Manta output stream threads.
     */
    private static final ThreadGroup THREAD_GROUP = new ThreadGroup("manta-outputstream");

    /**
     * Number of milliseconds to wait between checks to see if the stream has been closed.
     */
    private static final long CLOSED_CHECK_INTERVAL = 50L;


    /* Note: Do not turn this into a lambda expression - as of now it causes
     * a compilation error. */
    /**
     * Unhandled exception handler that logs errors that happened when reading
     * from the {@link InputStream}.
     */
    private static final Thread.UncaughtExceptionHandler EXCEPTION_HANDLER =
            new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(final Thread t, final Throwable e) {
                    String msg = String.format("An error occurred in the "
                            + "reading thread [%s] when attempting to "
                            + "write to an object via an OutputStream.",
                            t.getName());
                    LOGGER.error(msg, e);
                }
            };

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
            thread.setUncaughtExceptionHandler(EXCEPTION_HANDLER);

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
    private AtomicLong bytesWritten = new AtomicLong(0L);

    /**
     * Flag indicating that this stream has been closed.
     */
    private AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Path of the object being written to.
     */
    private final String path;

    /**
     * Creates a new instance of an {@link OutputStream} that wraps PUT
     * requests to Manta.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param httpHelper reference to HTTP operations helper class
     * @param mantaHttpHeaders optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @param contentType HTTP Content-Type header value
     */
    MantaObjectOutputStream(final String path, final HttpHelper httpHelper,
                            final MantaHttpHeaders mantaHttpHeaders,
                            final MantaMetadata metadata,
                            final ContentType contentType) {
        this.httpContent = new EmbeddedHttpContent(contentType.toString(),
                closed);
        this.path = path;

        final MantaHttpHeaders headers;

        if (mantaHttpHeaders == null) {
            headers = new MantaHttpHeaders();
        } else {
            headers = mantaHttpHeaders;
        }

        if (contentType != null) {
            headers.setContentType(contentType.toString());
        }

        /*
         * Thread execution definition that runs the HTTP PUT operation.
         */
        this.completed = EXECUTOR.submit(() ->
                httpHelper.httpPut(path, headers, httpContent, metadata));

        /*
         * We have to wait here until the upload to Manta starts and a writer
         * becomes available. The notify event that triggers the wait to stop
         * is in EmbeddedHttpContent. writeTo() after the inner OutputStream
         * (writer) has been set. We are forced to wait in the constructor like
         * this because the writer is needed in order to provide the
         * OutputStream API.
         */
        synchronized (this.httpContent) {
            while (httpContent.getWriter() == null && !isClosed()) {
                try {
                    /* We poll because the httpContent.notify() call within the
                     * httpContent.writeTo() method may have been called before
                     * the httpContent.wait() call below. */
                    this.httpContent.wait(CLOSED_CHECK_INTERVAL);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    @Override
    public void write(final int b) throws IOException {
        // Note: isClosed() is an expensive check, see note inside method
        if (isClosed()) {
            MantaIOException e = new MantaIOException("Can't write to a closed stream");
            e.setContextValue("path", path);
            throw e;
        }

        httpContent.getWriter().write(b);
        bytesWritten.incrementAndGet();
    }

    @Override
    public void write(final byte[] b) throws IOException {
        // Note: isClosed() is an expensive check, see note inside method
        if (isClosed()) {
            MantaIOException e = new MantaIOException("Can't write to a closed stream");
            e.setContextValue("path", path);
            throw e;
        }

        httpContent.getWriter().write(b);
        bytesWritten.addAndGet(b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        // Note: isClosed() is an expensive check, see note inside method
        if (isClosed()) {
            MantaIOException e = new MantaIOException("Can't write to a closed stream");
            e.setContextValue("path", path);
            throw e;
        }

        httpContent.getWriter().write(b, off, len);
        bytesWritten.addAndGet(b.length);
    }

    @Override
    public void flush() throws IOException {
        // Note: isClosed() is an expensive check, see note inside method
        if (isClosed()) {
            return;
        }

        httpContent.getWriter().flush();
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
        OutputStream inner = findMostInnerOutputStream(stream);

        // If the inner most stream is a closed instance, then we can assume
        // the stream is close.
        if (inner.getClass().equals(ClosedOutputStream.class)) {
            return true;
        }

        try {
            Field f = FieldUtils.getField(inner.getClass(), "closed", true);

            if (f == null) {
                throw new IllegalArgumentException("FieldUtils.getField(inner.getClass()) "
                        + "returned null");
            }

            Object result = f.get(inner);
            return  (boolean)result;
        } catch (IllegalArgumentException | IllegalAccessException | ClassCastException e) {
            String msg = String.format("Error finding [closed] field on class: %s",
                    inner.getClass());
            LOGGER.warn(msg, e);
            /* If we don't have an inner field called closed, it is inaccessible or
             * the field isn't a boolean, return null because we are now dealing with
             * undefined behavior. */
            return null;
        }
    }

    /**
     * <p>Finds the most inner stream if the embedded stream is stored on the passed
     * stream as a field named <code>out</code>. This hold true for all classes
     * that extend {@link java.io.FilterOutputStream}.</p>
     *
     * <p>NOTE: This is a dirty hack.</p>
     *
     * @param stream stream to search for inner stream
     * @return reference to inner stream class
     */
    protected static OutputStream findMostInnerOutputStream(final OutputStream stream) {
        Field f = FieldUtils.getField(stream.getClass(), "out", true);

        if (f == null) {
            return stream;
        } else {
            try {
                Object result = f.get(stream);

                if (result instanceof OutputStream) {
                    return findMostInnerOutputStream((OutputStream) result);
                } else {
                    return stream;
                }
            } catch (IllegalAccessException e) {
                // If we can't access the field, then we just return back the original stream
                return stream;
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>NOTE: This close method calls flush() on the inner {@link OutputStream}
     * and then closes it if possible.</p>
     *
     * @throws IOException
     */
    @Override
    public synchronized void close() throws IOException {
        this.closed.compareAndSet(false, true);

        Boolean innerIsClosed = isInnerStreamClosed(this.httpContent.getWriter());
        if (innerIsClosed != null && !innerIsClosed) {
            this.httpContent.getWriter().flush();
        }

        IOUtils.closeQuietly(this.httpContent);

        /* Now that we have marked the httpContent instance as properly closed,
         * we wake up the the sleeping thread in order for it properly exit the
         * writeTo() method in which it was waiting. */
        synchronized (this.httpContent) {
            /* We notify the streaming thread which is waiting within
             * EmbeddedHttpContent.writeTo() in order for it to wake up
             * and exit the thread. */
            this.httpContent.notify();
        }

        try {
            this.objectResponse = this.completed.get();
            this.objectResponse.setContentLength(bytesWritten.longValue());
        } catch (InterruptedException e) {
            // continue execution if interrupted
        } catch (ExecutionException e) {
            /* We wrap the cause because the stack trace for the
             * ExecutionException offers nothing useful and is just a wrapper
             * for exceptions that are thrown within a Future. */
            MantaIOException mioe = new MantaIOException(
                    "An exception was thrown within the thread writing to the network socket",
                    e.getCause());

            if (this.objectResponse != null) {
                final String requestId = this.objectResponse.getHeaderAsString(
                        MantaHttpHeaders.REQUEST_ID);

                if (requestId != null) {
                    mioe.addContextValue("requestId", requestId);
                }
            }

            mioe.addContextValue("path", path);

            throw mioe;
        }
    }

    /**
     * Flag indicating if the stream has been closed.
     *
     * @return true if closed, otherwise false
     */
    public boolean isClosed() {
        if (!closed.get()) {
            return closed.get();
        }

        final Boolean innerClosed;

        /* Note: There is a performance cost here that we are paying in exchange
         * for correctness. Every execution of isInnerStreamClosed() uses
         * reflection to find out if the innermost OutputStream is in fact
         * closed. */
        if (httpContent != null && httpContent.getWriter() != null) {
             innerClosed = isInnerStreamClosed(httpContent.getWriter());
        } else {
            innerClosed = false;
        }

        if (BooleanUtils.toBoolean(innerClosed)) {
            return closed.compareAndSet(false, innerClosed);
        }

        return closed.get();
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
