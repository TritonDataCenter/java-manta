/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests the functionality of signing private Manta URLs for public access.
 *
 * @author Elijah Zupancic
 */
@Test
public class MantaClientSigningIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    private ConfigContext config;

    @BeforeClass()
    public void beforeClass() throws IOException {

        // Let TestNG configuration take precedence over environment variables
        config = new IntegrationTestConfigContext();

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @Test
    public final void testCanCreateSignedGETUriFromPath() throws IOException {
        if (config.isClientEncryptionEnabled()) {
            throw new SkipException("Signed URLs are not decrypted by the client");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);

        // This will throw an error if the newly inserted object isn't present
        mantaClient.head(path);

        Instant expires = Instant.now().plus(1, ChronoUnit.HOURS);
        URI uri = mantaClient.getAsSignedURI(path, "GET", expires);

        HttpURLConnection connection = (HttpURLConnection)uri.toURL().openConnection();

        try (InputStream is = connection.getInputStream()) {
            connection.setReadTimeout(3000);
            connection.connect();

            if (connection.getResponseCode() != 200) {
                Assert.fail(IOUtils.toString(connection.getErrorStream(), Charset.defaultCharset()));
            }

            String actual = IOUtils.toString(is, Charset.defaultCharset());

            Assert.assertEquals(actual, TEST_DATA);
        } finally {
            connection.disconnect();
        }
    }

    @Test
    public final void testCanCreateSignedHEADUriFromPath() throws IOException {
        if (config.isClientEncryptionEnabled()) {
            throw new SkipException("Signed URLs are not decrypted by the client");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);

        // This will throw an error if the newly inserted object isn't present
        mantaClient.head(path);

        Instant expires = Instant.now().plus(1, ChronoUnit.HOURS);
        URI uri = mantaClient.getAsSignedURI(path, "HEAD", expires);

        HttpURLConnection connection = (HttpURLConnection)uri.toURL().openConnection();

        try {
            connection.setReadTimeout(3000);
            connection.setRequestMethod("HEAD");
            connection.connect();

            Map<String, List<String>> headers = connection.getHeaderFields();

            if (connection.getResponseCode() != 200) {
                Assert.fail(IOUtils.toString(connection.getErrorStream(), Charset.defaultCharset()));
            }

            Assert.assertNotNull(headers);
            Assert.assertEquals(TEST_DATA.length(), connection.getContentLength());
        } finally {
            connection.disconnect();
        }
    }

    @Test
    public final void testCanCreateSignedPUTUriFromPath()
            throws IOException, InterruptedException {
        if (config.isClientEncryptionEnabled()) {
            throw new SkipException("Signed URLs are not decrypted by the client");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        Instant expires = Instant.now().plus(1, ChronoUnit.HOURS);
        URI uri = mantaClient.getAsSignedURI(path, "PUT", expires);

        HttpURLConnection connection = (HttpURLConnection)uri.toURL().openConnection();

        connection.setReadTimeout(3000);
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        connection.setChunkedStreamingMode(10);
        connection.connect();

        try (OutputStreamWriter out = new OutputStreamWriter(
                connection.getOutputStream(), StandardCharsets.UTF_8)) {
            out.write(TEST_DATA);
        } finally {
            connection.disconnect();
        }

        // Wait for file to become available
        for (int i = 0; i < 10; i++ ) {
            Thread.sleep(500);

            if (mantaClient.existsAndIsAccessible(path)) {
                break;
            }
        }

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA);
    }

    @Test
    public final void testCanCreateSignedOPTIONSUriFromPath()
            throws IOException, InterruptedException {
        if (config.isClientEncryptionEnabled()) {
            throw new SkipException("Signed URLs are not decrypted by the client");
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);

        // This will throw an error if the newly inserted object isn't present
        mantaClient.head(path);

        Assert.assertEquals(mantaClient.getAsString(path), TEST_DATA);

        Instant expires = Instant.now().plus(1, ChronoUnit.HOURS);
        URI uri = mantaClient.getAsSignedURI(path, "OPTIONS", expires);

        HttpURLConnection connection = (HttpURLConnection)uri.toURL().openConnection();

        try {
            connection.setReadTimeout(3000);
            connection.setRequestMethod("OPTIONS");
            connection.connect();

            Map<String, List<String>> headers = connection.getHeaderFields();

            if (connection.getResponseCode() != 200) {
                String errorText = IOUtils.toString(connection.getErrorStream(), Charset.defaultCharset());

                if (config.getMantaUser().contains("/")) {
                    String msg = String.format("This fails due to an outstanding bug: MANTA-2839.\n%s",
                            errorText);
                    throw new SkipException(msg);
                }

                Assert.fail(errorText);
            }

            Assert.assertNotNull(headers);
            Assert.assertEquals(headers.get("Server").get(0), "Manta");
        } finally {
            connection.disconnect();
        }
    }
}
