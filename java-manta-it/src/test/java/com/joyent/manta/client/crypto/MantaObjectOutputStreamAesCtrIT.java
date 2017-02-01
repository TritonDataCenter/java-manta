package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectOutputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.*;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Properties;
import java.util.UUID;

@Test
public class MantaObjectOutputStreamAesCtrIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    public void beforeClass() throws IOException {

        Properties properties = new Properties();
        properties.put("manta.client_encryption", "true");
        properties.put("manta.encryption_algorithm", "AES256/CTR/NoPadding");
        properties.put("manta.encryption_key_id", "test");
        properties.put("manta.encryption_key_bytes_base64", "RkZGRkZGRkJEOTY3ODNDNkM5MUUyMjIyMTExMTIyMjI=");

        ConfigContext config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new EnvVarConfigContext(),
                new MapConfigContext(properties));

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
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
            out.write(TEST_DATA.getBytes());
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

        MantaObjectResponse head = mantaClient.head(path);
        AssertJUnit.assertNotNull(head.getHeader("m-encrypt-type"));
        AssertJUnit.assertEquals(head.getHeader("m-encrypt-cipher"), "AES256/CTR/NoPadding");

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

        MantaObjectResponse head = mantaClient.head(path);
        AssertJUnit.assertNotNull(head.getHeader("m-encrypt-type"));
        AssertJUnit.assertEquals(head.getHeader("m-encrypt-cipher"), "AES256/CTR/NoPadding");

        try (InputStream in = mantaClient.getAsInputStream(path)) {
            byte[] expected = bout.toByteArray();
            byte[] actual = IOUtils.readFully(in, (int)totalBytes);

            AssertJUnit.assertArrayEquals("Bytes written via OutputStream don't match read bytes",
                    expected, actual);
        }
    }
}
