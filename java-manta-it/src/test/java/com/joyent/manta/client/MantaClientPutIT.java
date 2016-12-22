package com.joyent.manta.client;

import com.joyent.manta.benchmark.RandomInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;

@Test
public class MantaClientPutIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private MantaClient mantaClient;

    private String testPathPrefix;


    @BeforeClass()
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }


    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @Test
    public final void testPutWithStringUTF8() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaObject response = mantaClient.put(path, TEST_DATA, Charsets.UTF_8);
        String contentType = response.getContentType();
        Assert.assertEquals(contentType, "text/plain; charset=UTF-8",
                "Content type wasn't detected correctly");

        try (MantaObjectInputStream object = mantaClient.getAsInputStream(path)) {
            String actual = IOUtils.toString(object, Charsets.UTF_8);
            String contentMd5 = object.getHttpHeaders().getFirstHeaderStringValue("content-md5");

            Assert.assertEquals(actual, TEST_DATA,
                    "Uploaded string didn't match expectation");

            Assert.assertEquals(contentMd5, "FsdpTF2dV/QZ50ylTCIA1w==",
                    "Content MD5 returned was incorrect");
        }
    }


    @Test
    public final void testPutWithStringUTF16() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        MantaObject response = mantaClient.put(path, TEST_DATA, Charsets.UTF_16);
        String contentType = response.getContentType();
        Assert.assertEquals(contentType, "text/plain; charset=UTF-16",
                "Content type wasn't detected correctly");

        try (MantaObjectInputStream object = mantaClient.getAsInputStream(path)) {
            String actual = IOUtils.toString(object, Charsets.UTF_16);
            String contentMd5 = object.getHttpHeaders().getFirstHeaderStringValue("content-md5");

            Assert.assertEquals(actual, TEST_DATA,
                    "Uploaded string didn't match expectation");

            Assert.assertEquals(contentMd5, "YZBaIz8cxVhRT0Vpb/nFXA==",
                    "Content MD5 returned was incorrect");
        }
    }


    @Test
    public final void testPutWithRetryableStream() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        try (InputStream testDataInputStream = classLoader.getResourceAsStream(TEST_FILENAME)) {
            Assert.assertTrue(testDataInputStream.markSupported(),
                    "Mark should be supported so that it computes computed-md5 before sending");
            mantaClient.put(path, testDataInputStream);
            MantaObjectResponse head = mantaClient.head(path);
            String contentMd5 = head.getHttpHeaders().getFirstHeaderStringValue("content-md5");

            Assert.assertEquals(contentMd5, "aa3zMQAwSpnbQMpk26h4Aw==",
                    "Content MD5 returned was incorrect");
        }
    }

    @Test
    public final void testPutWithNonRetryableStream() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        try (InputStream testDataInputStream = new RandomInputStream(1024L)) {
            Assert.assertFalse(testDataInputStream.markSupported(),
                    "Mark should be not be supported so that it doesn't compute computed-md5");
            mantaClient.put(path, testDataInputStream);
        }
    }

    @Test
    public final void testPutWithPlainTextFile() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        File temp = File.createTempFile("upload", ".txt");

        try {
            Files.write(temp.toPath(), TEST_DATA.getBytes(Charsets.UTF_8));
            MantaObject response = mantaClient.put(path, temp);
            String contentType = response.getContentType();
            Assert.assertEquals(contentType, "text/plain",
                    "Content type wasn't detected correctly");
        } finally {
            Files.delete(temp.toPath());
        }

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded file didn't match expectation");
    }

    @Test
    public final void testPutWithJPGFile() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        File temp = File.createTempFile("upload", ".jpg");
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        try (InputStream testDataInputStream = classLoader.getResourceAsStream(TEST_FILENAME);
             OutputStream out = new FileOutputStream(temp)) {
            IOUtils.copy(testDataInputStream, out);
            MantaObject response = mantaClient.put(path, temp);
            String contentType = response.getContentType();
            Assert.assertEquals(contentType, "image/jpeg",
                    "Content type wasn't detected correctly");
        } finally {
            Files.delete(temp.toPath());
        }

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded file didn't match expectation");
    }
}
