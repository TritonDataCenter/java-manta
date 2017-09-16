package com.joyent.manta.client;

import com.joyent.http.signature.KeyFingerprinter;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.OnCloseAggregateException;
import com.joyent.manta.util.InfiniteInputStream;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ProxyInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.ConnectionClosedException;
import org.apache.http.impl.execchain.RequestAbortedException;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class MantaClientCloseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantaClientCloseTest.class);

    private static boolean isSocketException(Exception e) {
        if (e instanceof SocketException) {
            return true;
        }

        if (e instanceof IOException
                && e.getCause() instanceof ExecutionException
                && e.getCause().getCause() instanceof SocketException) {
            return true;
        }

        if (e instanceof RequestAbortedException) {
            return true;
        }

        if (e instanceof InterruptedIOException) {
            return true;
        }

        if (e instanceof ConnectionClosedException) {
            return true;
        }

        return false;
    }

    private static class Downloader implements Runnable {

        private final MantaClient client;
        private final CountDownLatch latch;
        private final Queue<Exception> exceptions;

        private Downloader(final MantaClient client, final CountDownLatch latch, final Queue<Exception> exceptions) {
            this.client = client;
            this.latch = latch;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("downloader-" + client.hashCode());
            try {
                final MantaObjectInputStream is = client.getAsInputStream("/arbitrary/path");
                latch.countDown();
                LOGGER.debug("downloader ready");
                IOUtils.copy(is, NullOutputStream.NULL_OUTPUT_STREAM);
            } catch (Exception e) {
                if (isSocketException(e)) {
                    return;
                }

                LOGGER.error("downloader failed unexpectedly ", e);
                exceptions.add(e);
            }
        }
    }

    private static class Uploader implements Runnable {

        private final MantaClient client;
        private final CountDownLatch latch;
        private final Queue<Exception> exceptions;

        private Uploader(final MantaClient client, final CountDownLatch latch, final Queue<Exception> exceptions) {
            this.client = client;
            this.latch = latch;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("uploader-" + client.hashCode());
            try {
                final AtomicBoolean counted = new AtomicBoolean(false);
                client.put("/arbitrary/path", new ProxyInputStream(new InfiniteInputStream(new byte[]{'a'})) {
                    @Override
                    protected void afterRead(int n) throws IOException {
                        if (!counted.getAndSet(true)) {
                            latch.countDown();
                            LOGGER.debug("uploader ready");
                        }
                    }
                });
            } catch (Exception e) {
                if (isSocketException(e)) {
                    return;
                }

                LOGGER.error("uploader failed unexpectedly", e);
                exceptions.add(e);
            }
        }
    }

    private static class Terminator implements Runnable {

        private final MantaClient client;
        private final CountDownLatch latch;
        private final Queue<Exception> exceptions;

        private Terminator(final MantaClient client, final CountDownLatch latch, final Queue<Exception> exceptions) {
            this.client = client;
            this.latch = latch;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            Thread.currentThread().setName("terminator-" + client.hashCode());
            try {
                // wait for requests to be in-flight
                LOGGER.debug("terminator waiting");
                latch.countDown();
                if (!latch.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("terminator had to wait too long for other threads");
                }
                LOGGER.debug("terminator closing client");
                client.close();
            } catch (OnCloseAggregateException aggE) {
                // we should look through the close exceptions to see if anything weird occurred
                for (Exception e : aggE.exceptions()) {
                    if (!(e instanceof SocketException)) {
                        exceptions.add(aggE);
                        break;
                    }
                }

            } catch (Exception e) {
                LOGGER.error("terminator failed unexpectedly", e);
                exceptions.add(e);
            }
        }
    }

    @Test(invocationCount = 100)
    public void testFastCloseCausesExpectedExceptions() throws Exception {
        final HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        LOGGER.debug("fast-close enabled test using port: " + server.getAddress().getPort());

        server.setExecutor(Executors.newFixedThreadPool(2));
        server.createContext("/", (exchange) -> {
            LOGGER.debug(new StringBuilder("HTTP request: ")
                    .append(exchange.getProtocol())
                    .append(" ")
                    .append(exchange.getRequestMethod())
                    .toString());
            // block indefinitely

            if (exchange.getRequestMethod().equalsIgnoreCase("PUT")) {
                LOGGER.debug("Stub server received PUT, discarding infinitely");
                exchange.sendResponseHeaders(201, 0);
                IOUtils.copy(exchange.getRequestBody(), NullOutputStream.NULL_OUTPUT_STREAM);
            }

            if (!exchange.getRequestMethod().equalsIgnoreCase("GET")) {
                LOGGER.error("Stub server received unexpected request method " + exchange.getRequestMethod());
                exchange.getRequestBody().close();
                exchange.getResponseBody().close();
                exchange.close();
                return;
            }

            LOGGER.debug("Stub server received GET, writing infinitely");
            exchange.sendResponseHeaders(200, 0);
            final InputStream response = new InfiniteInputStream(new byte[]{'a'});
            IOUtils.copy(response, exchange.getResponseBody());
        });
        server.start();
        LOGGER.debug("Started stub server");

        final ImmutablePair<File, String> key = generatePrivateKey();

        final MantaClient client = new MantaClient(
                new ChainedConfigContext(
                        new DefaultsConfigContext(),
                        new StandardConfigContext()
                                .setMantaUser("user")
                                .setMantaKeyPath(key.left.getAbsolutePath())
                                .setMantaKeyId(key.right)
                                .setMantaURL("http://localhost:" + server.getAddress().getPort())
                                .setMaximumConnections(2)
                                .setFastCloseEnabled(true)));

        final ExecutorService pool = Executors.newFixedThreadPool(3);
        final ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        final CountDownLatch latch = new CountDownLatch(3);

        pool.submit(new Downloader(client, latch, exceptions));
        pool.submit(new Uploader(client, latch, exceptions));
        pool.submit(new Terminator(client, latch, exceptions));

        latch.await();
        LOGGER.debug("shutting down pool");
        pool.shutdown();

        if (pool.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.debug("pool terminated within the expected time");
        } else {
            LOGGER.info("Forced to terminate worker pool");
            pool.shutdownNow();
            client.closeQuietly();
            exceptions.add(new IllegalStateException("main thread was forced to terminate worker pool"));
        }

        final String exceptionMessage = "unexpected exception count: " + exceptions.size();
        LOGGER.debug(exceptionMessage);
        if (exceptions.isEmpty()) {
            return;
        }

        final OnCloseAggregateException aggException = new OnCloseAggregateException();

        if (!client.isClosed()) {
            client.closeQuietly();
            exceptions.add(new IllegalStateException("main thread had to close client"));
        }

        for (Exception e : exceptions) {
            aggException.aggregateException(e);
        }

        Assert.fail(exceptionMessage, aggException);
    }

    private ImmutablePair<File, String> generatePrivateKey() throws IOException {
        final KeyPair keyPair;
        try {
            keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        } catch (NoSuchAlgorithmException impossible) {
            throw new RuntimeException(impossible); // "RSA" is always provided
        }

        final File keyFile = File.createTempFile("private-key", "");
        FileUtils.forceDeleteOnExit(keyFile);

        try (final FileWriter fileWriter = new FileWriter(keyFile);
             final JcaPEMWriter writer = new JcaPEMWriter(fileWriter)) {

            writer.writeObject(keyPair.getPrivate());
            writer.flush();
        }

        return new ImmutablePair<>(keyFile, KeyFingerprinter.md5Fingerprint(keyPair));
    }
}
