/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests the proper functioning of the dynamically paging iterator.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaDirectoryIteratorIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    private ConfigContext config;

    private HttpRequestFactoryProvider httpRequestFactoryProvider;

    private HttpHelper httpHelper;

    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());

        mantaClient.putDirectory(testPathPrefix);

        httpRequestFactoryProvider = mantaClient.getHttpRequestFactoryProvider();
        httpHelper = new HttpHelper(config.getMantaURL(),
                httpRequestFactoryProvider.getRequestFactory());
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeQuietly();
        }
    }

    @Test
    public void isPagingCorrectly() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        final int MAX = 30;

        // Add files 1-30
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            mantaClient.put(path, TEST_DATA);
        }

        String url = config.getMantaURL();

        try (MantaDirectoryIterator itr = new MantaDirectoryIterator(url,
                dir, httpHelper, 5)) {
            // Make sure we can get the first element
            Assert.assertTrue(itr.hasNext(), "We should have the first element");
            Map<String, Object> first = itr.next();
            Assert.assertEquals(first.get("name").toString(), "00001");

            // Scroll forward to the last element
            for (int i = 2; i < MAX; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), String.format("%05d", i));
            }

            // Make sure that we can get the last element
            Assert.assertTrue(itr.hasNext(), "We should have the last element");
            Map<String, Object> last = itr.next();
            Assert.assertEquals(last.get("name").toString(), String.format("%05d", MAX));

            // Make sure that we are at the end of the iteration
            Assert.assertFalse(itr.hasNext());

            boolean failed = false;

            try {
                itr.next();
            } catch (NoSuchElementException e) {
                failed = true;
            }

            Assert.assertTrue(failed, "Iterator failed to throw NoSuchElementException");
        }
    }

    @Test
    public void isPagingConcurrentlyCorrectly() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        final int MAX = 300;
        final Map<String, Boolean> valuesFound = new ConcurrentHashMap<>(MAX);

        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            valuesFound.put(name, false);
            mantaClient.put(path, TEST_DATA);
        }

        String url = config.getMantaURL();

        try (MantaDirectoryIterator itr = new MantaDirectoryIterator(url,
                dir, httpHelper, 10)) {

            Runnable search = () -> {
                while (itr.hasNext()) {
                    try {
                        String name = itr.next().get("name").toString();
                        valuesFound.replace(name, false, true);
                    } catch (NoSuchElementException e) {
                        /* we don't care about these exceptions because it is
                         * just another thread beating us to the end. */
                    }
                }
            };

            // Start 3 threads that iterate at the same time
            Thread thread1 = new Thread(search);
            Thread thread2 = new Thread(search);
            Thread thread3 = new Thread(search);

            thread1.start();
            thread2.start();
            thread3.start();

            // Wait in the current thread for them all to exit
            while (itr.hasNext()) {
                Thread.sleep(1000);

                if (!thread1.isAlive() && !thread2.isAlive() && !thread3.isAlive()) {
                    // one last check
                    if (itr.hasNext()) {
                        throw new AssertionError("We are in a dead-lock or a bad state");
                    }
                }
            }

            // Validate that all files were found
            valuesFound.entrySet().stream().forEach(m -> Assert.assertTrue(m.getValue()));
        } catch (InterruptedException e) {
            afterClass();
        }
    }

    @Test
    public void canListEmptyDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        String url = config.getMantaURL();

        try (MantaDirectoryIterator itr = new MantaDirectoryIterator(url,
                dir, httpHelper, 10)) {
            Assert.assertFalse(itr.hasNext(), "There shouldn't be a next element");

            boolean failed = false;

            try {
                itr.next();
            } catch (NoSuchElementException e) {
                failed = true;
            }

            Assert.assertTrue(failed, "Iterator failed to throw NoSuchElementException");
        }
    }

    @Test
    public void canListDirectoryUsingSmallPagingSize() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        final int MAX = 5;

        // Add files 1-5
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            mantaClient.put(path, TEST_DATA);
        }

        String url = config.getMantaURL();

        try (MantaDirectoryIterator itr = new MantaDirectoryIterator(url,
                dir, httpHelper, 2)) {

            for (int i = 1; i < MAX; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), String.format("%05d", i));
            }
        }
    }
}
