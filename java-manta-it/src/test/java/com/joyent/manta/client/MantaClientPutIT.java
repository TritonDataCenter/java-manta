package com.joyent.manta.client;

import com.joyent.manta.benchmark.RandomInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
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
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

@Test
public class MantaClientPutIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private MantaClient mantaClient;

    private String testPathPrefix;


    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

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
    public final void testPutWithMarkSupportedStream() throws IOException, URISyntaxException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        final String expectedMd5 = "aa3zMQAwSpnbQMpk26h4Aw==";

        try (InputStream testDataInputStream = classLoader.getResourceAsStream(TEST_FILENAME);
             CountingInputStream countingInputStream = new CountingInputStream(testDataInputStream)) {
            Assert.assertTrue(countingInputStream.markSupported());
            mantaClient.put(path, countingInputStream);
            MantaObjectResponse head = mantaClient.head(path);
            String contentMd5 = head.getHttpHeaders().getFirstHeaderStringValue("content-md5");


            if (!contentMd5.equals(expectedMd5)) {
                Path localFile = Paths.get(classLoader.getResource(TEST_FILENAME).toURI());

                long expectedLength = Files.size(localFile);
                long actualLength = head.getContentLength();

                String msg = String.format("Content MD5 returned was incorrect.\n"
                    + "Expected MD5 : %s\n"
                    + "Actual MD5   : %s\n"
                    + "Expected Content Length   : %d\n"
                    + "Bytes Read Content Length : %d\n"
                    + "Actual Content Length     : %d\n",
                    expectedMd5, contentMd5, expectedLength,
                    countingInputStream.getByteCount(), actualLength);

                Assert.fail(msg);
            }
        }
    }

    @Test
    public final void testPutWithStreamThatDoesntFitInBuffer() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        final int length = mantaClient.getContext().getUploadBufferSize() + 1024;

        try (InputStream testDataInputStream = new RandomInputStream(length)) {
            Assert.assertFalse(testDataInputStream.markSupported());
            mantaClient.put(path, testDataInputStream);
        }
    }

    @Test
    public final void testPutWithStreamAndKnownContentLength() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        final int length = 20 * 1024;
        try (InputStream testDataInputStream = new RandomInputStream(length)) {
            mantaClient.put(path, testDataInputStream, length, null, null);
        }
    }

    @Test
    public final void testPutWithStreamAndKnownContentLengthBeyondBuffer() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        final int length = mantaClient.getContext().getUploadBufferSize() + 1024;
        try (InputStream testDataInputStream = new RandomInputStream(length)) {
            mantaClient.put(path, testDataInputStream, length, null, null);
        }
    }

    @Test
    public final void testPutWithByteArray() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = TEST_DATA.getBytes(Charsets.UTF_8);

        MantaObject response = mantaClient.put(path, content);
        String contentType = response.getContentType();
        Assert.assertEquals(contentType, "application/octet-stream",
                "Content type wasn't set to expected default");

        final String actual = mantaClient.getAsString(path, Charsets.UTF_8);

        Assert.assertEquals(actual, TEST_DATA,
                "Uploaded byte array was malformed");
    }

    @Test
    public final void testPutWithPlainTextFileUTF8RomanCharacters() throws IOException {
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
    public final void testPutWithPlainTextFileUTF8NonRomanCharacters() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        File temp = File.createTempFile("upload", ".txt");

        final String content = "これは日本語です。";

        try {
            Files.write(temp.toPath(), content.getBytes(Charsets.UTF_8));
            MantaObject response = mantaClient.put(path, temp);
            String contentType = response.getContentType();
            Assert.assertEquals(contentType, "text/plain",
                    "Content type wasn't detected correctly");
        } finally {
            Files.delete(temp.toPath());
        }

        String actual = mantaClient.getAsString(path);
        Assert.assertEquals(actual, content,
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


            try (InputStream in = mantaClient.getAsInputStream(path)) {
                byte[] actual = IOUtils.toByteArray(in);
                byte[] expected = FileUtils.readFileToByteArray(temp);

                Assert.assertTrue(Arrays.equals(actual, expected),
                        "Uploaded file isn't the same as actual file");
            }
        } finally {
            Files.delete(temp.toPath());
        }
    }
}
