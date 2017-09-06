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
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MantaClientCloseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MantaClientCloseTest.class);

    @Test
    public void testHttpServer() throws Exception {
        final int port = RandomUtils.nextInt(3000, 30000);
        LOGGER.debug("using port: " + port);
        final HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(2));
        server.createContext("/", (exchange) -> {
            LOGGER.debug(new StringBuilder("HTTP request: ")
                    .append(exchange.getProtocol())
                    .append(" ")
                    .append(exchange.getRequestMethod())
                    .toString());
            // block indefinitely

            if (exchange.getRequestMethod().equalsIgnoreCase("post")) {
                exchange.sendResponseHeaders(201, 0);
                exchange.close();
                return;
            }

            exchange.sendResponseHeaders(200, 0);
            final OutputStream responseBody = exchange.getResponseBody();

            final InputStream response = new InfiniteInputStream(new byte[]{'a'});
            while (true) {
                IOUtils.copy(response, responseBody);
            }

        });
        server.start();
        LOGGER.debug("started");

        final ImmutablePair<File, String> key = generatePrivateKey();
        final int timeoutSeconds = 5;

        final MantaClient client = new MantaClient(
                new ChainedConfigContext(
                        new DefaultsConfigContext(),
                        new StandardConfigContext()
                                .setMantaUser("user")
                                .setMantaKeyPath(key.left.getAbsolutePath())
                                .setMantaKeyId(key.right)
                                .setMantaURL("http://localhost:" + port)
                                .setTimeout(timeoutSeconds * 1000)
                                .setMaximumConnections(4)));

        final ExecutorService pool = Executors.newFixedThreadPool(client.getContext().getMaximumConnections());
        final ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        final CountDownLatch latch = new CountDownLatch(1);

        final Runnable downloader = () -> {
            Thread.currentThread().setName("downloader");
            LOGGER.debug("downloader getting in the pool");
            try {
                final MantaObjectInputStream is = client.getAsInputStream("/arbitrary/path");
                latch.countDown();
                IOUtils.copy(is, NullOutputStream.NULL_OUTPUT_STREAM);
            } catch (Exception e) {
                // LOGGER.error("Exception Caught while getting thing", e.getMessage());
                exceptions.add(e);
                return;
            }
            exceptions.add(new IllegalStateException("shouldn't make it here"));
        };

        final Runnable uploader = () -> {
            Thread.currentThread().setName("uploader");
            LOGGER.debug("uploader getting in the pool");
            try {
                final OutputStream mantaOut = client.putAsOutputStream("/arbitrary/path");
                latch.countDown();
                IOUtils.copy(new InfiniteInputStream(new byte[]{'a'}), mantaOut);
            } catch (Exception e) {
                // LOGGER.error("Exception Caught while getting thing", e.getMessage());
                exceptions.add(e);
                return;
            }
            exceptions.add(new IllegalStateException("shouldn't make it here"));
        };

        final Runnable terminator = () -> {
            Thread.currentThread().setName("terminator");
            // wait for requests to be in-flight
            try {
                if (!latch.await(5, TimeUnit.SECONDS)) {
                    exceptions.add(new IllegalStateException("terminator had to wait too long for other threads"));
                    return;
                }
                client.close();
                return;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            exceptions.add(new IllegalStateException("shouldn't make it here"));
        };

        pool.submit(downloader);
        pool.submit(uploader);
        pool.submit(terminator);

        latch.await();
        LOGGER.debug("waiting for terminator");
        TimeUnit.SECONDS.sleep(5);
        LOGGER.debug("shutting down pool");

        pool.shutdown();

        if (pool.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.debug("pool terminated within the expected time");
        } else {
            Assert.fail("Forced to terminate worker pool");
        }

        final String exceptionMessage = "exception count: " + exceptions.size();
        LOGGER.debug(exceptionMessage);
        if (exceptions.isEmpty()) {
            return;
        }

        final OnCloseAggregateException aggException = new OnCloseAggregateException();
        for (Exception e : exceptions) {
            aggException.aggregateException(e);
        }

        Assert.fail(exceptionMessage, aggException);
    }


    @Test(dataProvider = "threadCounts")
    public void testConcurrentClose(final int threadCount) throws NoSuchAlgorithmException, IOException, InterruptedException {
    }


    @DataProvider(name = "threadCounts")
    public Object[][] threadCounts() {
        return new Object[][]{
                new Object[]{1},
                new Object[]{10},
                // new Object[] {100},
                // new Object[] {1000},
        };
    }


    private ImmutablePair<File, String> generatePrivateKey() throws NoSuchAlgorithmException, IOException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();

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
