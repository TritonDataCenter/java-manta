/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tests the proper functioning of the dynamically paging iterator.
 */
@Test(groups={"directory"})
public class MantaDirectoryListingIteratorIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    public void beforeClass() throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        IntegrationTestConfigContext.cleanupTestDirectory(mantaClient, testPathPrefix);
    }

    public void isPagingCorrectly() throws IOException {
        String dir = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        final int MAX = 30;

        // Add files 1-30
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            mantaClient.put(path, TEST_DATA);
        }

        try (MantaDirectoryListingIterator itr = mantaClient.streamingIterator(dir, 5)) {
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
        String dir = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        final int MAX = 300;
        final Map<String, Boolean> valuesFound = new ConcurrentHashMap<>(MAX);

        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            valuesFound.put(name, false);
            mantaClient.put(path, TEST_DATA);
        }

        try (MantaDirectoryListingIterator itr = mantaClient.streamingIterator(dir, 10)) {
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
            valuesFound.entrySet().forEach(m -> Assert.assertTrue(m.getValue()));
        } catch (InterruptedException e) {
            afterClass();
        }
    }

    public void canListEmptyDirectory() throws IOException {
        String dir = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        try (MantaDirectoryListingIterator itr = mantaClient.streamingIterator(dir, 10)) {
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

    private void listDirectoryUsingSmallPagingSize(final String dir) throws IOException {
        mantaClient.putDirectory(dir, true);

        final int MAX = 5;

        // Add files 1-5
        for (int i = 1; i <= MAX; i++) {
            String name = String.format("%05d", i);
            String path = String.format("%s/%s", dir, name);

            mantaClient.put(path, TEST_DATA);
        }

        try (MantaDirectoryListingIterator itr = mantaClient.streamingIterator(dir, 2)) {
            for (int i = 1; i < MAX; i++) {
                Assert.assertTrue(itr.hasNext(), "We should have the next element");
                Map<String, Object> next = itr.next();
                Assert.assertEquals(next.get("name").toString(), String.format("%05d", i));
            }
        }
    }

    public void canListDirectoryUsingSmallPagingSize() throws IOException {
        String dir = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        listDirectoryUsingSmallPagingSize(dir);
    }

    @Test(enabled = false) // Triggers server side bug: MANTA-2409
    public void canListDirectoryUsingSmallPagingSizeAndErrorProneName() throws IOException {
        String dir = String.format("%s%s/%s", testPathPrefix, UUID.randomUUID(), "- -!@#$%^&*()");
        listDirectoryUsingSmallPagingSize(dir);
    }
}
