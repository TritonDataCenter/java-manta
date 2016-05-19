/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
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
public class MantaClientSigningIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    private ConfigContext config;

    @BeforeClass()
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout", "manta.http_transport"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout,
                            @Optional String mantaHttpTransport)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout,
                mantaHttpTransport);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @Test
    public final void testCanCreateSignedGETUriFromPath() throws IOException {
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
                Assert.fail(MantaUtils.inputStreamToString(connection.getErrorStream()));
            }

            String actual = MantaUtils.inputStreamToString(is);
            Assert.assertEquals(actual, TEST_DATA);
        } finally {
            connection.disconnect();
        }
    }

    @Test
    public final void testCanCreateSignedHEADUriFromPath() throws IOException {
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
                Assert.fail(MantaUtils.inputStreamToString(connection.getErrorStream()));
            }

            Assert.assertNotNull(headers);
            Assert.assertEquals(TEST_DATA.length(), connection.getContentLength());
        } finally {
            connection.disconnect();
        }
    }


    @Test
    public final void testCanCreateSignedPUTUriFromPath() throws IOException, InterruptedException {
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
                connection.getOutputStream())) {
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
    public final void testCanCreateSignedOPTIONSUriFromPath() throws IOException, InterruptedException {
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
                String errorText = MantaUtils.inputStreamToString(connection.getErrorStream());

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
