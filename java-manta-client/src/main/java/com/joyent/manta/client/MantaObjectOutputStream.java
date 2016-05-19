/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class MantaObjectOutputStream extends OutputStream {
    private static final Logger LOG = LoggerFactory.getLogger(MantaObjectOutputStream.class);

    protected static final ThreadGroup THREAD_GROUP = new ThreadGroup("manta-outputstream");

    protected static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(new ThreadFactory() {
        protected final AtomicInteger count = new AtomicInteger(1);

        protected final Thread.UncaughtExceptionHandler handler = (t, e) -> {
            if (LOG.isErrorEnabled()) {
                String msg = String.format("Unhandled OutputStream error on thread [%s]",
                        t.getName());
                LOG.error(msg, e);
            }
        };

        @Override
        public Thread newThread(final Runnable runnable) {
            final String name = String.format("stream-%d", count.getAndIncrement());
            Thread thread = new Thread(THREAD_GROUP, name);
            thread.setUncaughtExceptionHandler(handler);
            thread.setDaemon(true);

            return thread;
        }
    });

    protected class EmbeddedHttpContent implements HttpContent {
        protected volatile OutputStream writer;

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
            while (!isClosed) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    // continue execution
                }
            }
        }
    }

    private final String path;
    private final HttpHelper httpHelper;
    private final MantaHttpHeaders headers;
    private final MantaMetadata metadata;
    private final String contentType;
    private final EmbeddedHttpContent httpContent;
    private final Future<MantaObjectResponse> completed;

    private final Callable<MantaObjectResponse> upload = new Callable<MantaObjectResponse>() {
        @Override
        public MantaObjectResponse call() throws Exception {
            return httpHelper.httpPut(path, headers, httpContent, metadata);
        }
    };

    private volatile boolean isClosed = false;

    public MantaObjectOutputStream(final String path, final HttpHelper httpHelper,
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
    }

    @Override
    public void write(final int b) throws IOException {
        httpContent.writer.write(b);
    }

    @Override
    public void write(final byte[] b) throws IOException {
        httpContent.writer.write(b);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        httpContent.writer.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        httpContent.writer.flush();
    }

    @Override
    public void close() throws IOException {
        super.close();
        httpContent.writer.flush();
        this.isClosed = true;

        try {
            completed.get();
        } catch (InterruptedException e) {
            // continue execution if interrupted
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
    }
}
