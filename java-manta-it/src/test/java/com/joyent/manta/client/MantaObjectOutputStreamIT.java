/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.UUID;

@Test
public class MantaObjectOutputStreamIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public void canUploadSmallString() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);

        try {
            out.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        } finally {
            out.close();
        }

        MantaObject uploaded = out.getObjectResponse();

        Assert.assertEquals(uploaded.getContentLength().longValue(), TEST_DATA.length(),
                "Uploaded content length doesn't match");

        Assert.assertTrue(mantaClient.existsAndIsAccessible(path),
                "File wasn't uploaded: " + path);

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded bytes don't match");
    }

    public void canUploadSmallStringWithErrorProneName() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + "- -~!@#$%^&*().txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);

        try {
            out.write(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        } finally {
            out.close();
        }

        MantaObject uploaded = out.getObjectResponse();

        Assert.assertTrue(mantaClient.existsAndIsAccessible(path),
                "File wasn't uploaded: " + path);
    }

    public void canUploadMuchLargerFile() throws IOException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(6553600);

        MessageDigest md5Digest = DigestUtils.getMd5Digest();
        long totalBytes = 0;

        try {
            for (int i = 0; i < 100; i++) {
                int chunkSize = RandomUtils.nextInt(1, 131072);
                byte[] randomBytes = RandomUtils.nextBytes(chunkSize);
                md5Digest.update(randomBytes);
                totalBytes += randomBytes.length;
                out.write(randomBytes);
                bout.write(randomBytes);

                // periodically flush
                if (i % 25 == 0) {
                    out.flush();
                }
            }
        } finally {
            out.close();
            bout.close();
        }

        try (InputStream in = mantaClient.getAsInputStream(path)) {
            byte[] expected = bout.toByteArray();
            byte[] actual = IOUtils.readFully(in, (int)totalBytes);

            AssertJUnit.assertArrayEquals("Bytes written via OutputStream don't match read bytes",
                    expected, actual);
        }
    }

    public void canUploadMuchLargerFileWithPeriodicWaits() throws IOException, InterruptedException {
        String path = testPathPrefix + "uploaded-" + UUID.randomUUID() + ".txt";
        MantaObjectOutputStream out = mantaClient.putAsOutputStream(path);
        ByteArrayOutputStream bout = new ByteArrayOutputStream(6553600);

        MessageDigest md5Digest = DigestUtils.getMd5Digest();
        long totalBytes = 0;

        try {
            for (int i = 0; i < 100; i++) {
                int chunkSize = RandomUtils.nextInt(1, 131072);
                byte[] randomBytes = RandomUtils.nextBytes(chunkSize);
                md5Digest.update(randomBytes);
                totalBytes += randomBytes.length;
                out.write(randomBytes);
                bout.write(randomBytes);

                // periodically wait
                if (i % 3 == 0) {
                    Thread.sleep(RandomUtils.nextLong(1L, 1000L));
                }
            }
        } finally {
            out.close();
            bout.close();
        }

        try (InputStream in = mantaClient.getAsInputStream(path)) {
            byte[] expected = bout.toByteArray();
            byte[] actual = IOUtils.readFully(in, (int)totalBytes);

            AssertJUnit.assertArrayEquals("Bytes written via OutputStream don't match read bytes",
                    expected, actual);
        }
    }
}
