package com.joyent.manta.client;

import com.joyent.manta.benchmark.RandomInputStream;
import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class MantaMultipartIT {
    private MantaClient mantaClient;
    private MantaMultipart multipart;

    private String testPathPrefix;

    private Logger LOG = LoggerFactory.getLogger(getClass());

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
        ConfigContext config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout,
                mantaHttpTransport);

        this.mantaClient = new MantaClient(config);
        this.multipart = new MantaMultipart(this.mantaClient);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    public void nonExistentFileHasNotStarted() throws IOException {
        assertFalse(multipart.isStarted(new UUID(0L, -1L)));
    }

    public void canUploadSmallMultipartString() throws IOException {
        String[] parts = new String[] {
                "Hello ",
                "world ",
                "Joyent",
                "!"
        };

        StringBuilder combined = new StringBuilder();
        for (String p : parts) {
            combined.append(p);
        }

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path);

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(uploadId, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(uploadId);
        Instant start = Instant.now();
        multipart.complete(uploadId);
        multipart.waitForCompletion(uploadId);
        Instant end = Instant.now();

        assertTrue(multipart.isComplete(uploadId));
        assertFalse(multipart.isStarted(uploadId));

        assertEquals(mantaClient.getAsString(path),
                combined.toString(),
                "Manta combined string doesn't match expectation");

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Concatenating {} parts took {} seconds",
                parts.length, totalCompletionTime.toMillis() / 1000);
    }

    public void canStoreContentType() throws IOException {
        String[] parts = new String[] {
                "Hello ",
                "world ",
                "Joyent",
                "!"
        };

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final MantaHttpHeaders headers = new MantaHttpHeaders()
                .setContentType("text/plain");
        final UUID uploadId = multipart.initiateUpload(path, null, headers);

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(uploadId, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(uploadId);
        multipart.complete(uploadId);
        multipart.waitForCompletion(uploadId);

        MantaObjectResponse head = mantaClient.head(path);
        assertEquals(head.getContentType(), "text/plain",
                "Content type header wasn't set correctly");
    }

    public void canStoreMetadata() throws IOException {
        String[] parts = new String[] {
                "Hello ",
                "world ",
                "Joyent",
                "!"
        };

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-hello", "world");
        metadata.put("m-foo", "bar");

        final UUID uploadId = multipart.initiateUpload(path, metadata);

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(uploadId, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(uploadId);
        multipart.complete(uploadId);
        multipart.waitForCompletion(uploadId);

        MantaMetadata remoteMetadata = mantaClient.head(path).getMetadata();

        assertEquals(remoteMetadata.size(), 2, "Unexpected metadata size");
        assertEquals(remoteMetadata.get("m-hello"), "world");
        assertEquals(remoteMetadata.get("m-foo"), "bar");
    }

    public void canUpload5MBMultipartBinary() throws IOException {
        final long fiveMB = 5L * 1024L * 1024L;

        File[] parts = new File[] {
                createTemporaryDataFile(fiveMB, 1),
                createTemporaryDataFile(fiveMB, 1),
                createTemporaryDataFile(fiveMB, 1)
        };

        final File expectedFile = concatenateFiles(parts);
        final byte[] expectedMd5 = md5(expectedFile);

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path);

        for (int i = 0; i < parts.length; i++) {
            File part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(uploadId, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(uploadId);
        Instant start = Instant.now();
        multipart.complete(uploadId);
        multipart.waitForCompletion(uploadId);
        Instant end = Instant.now();

        assertTrue(multipart.isComplete(uploadId));
        assertFalse(multipart.isStarted(uploadId));

        MantaObjectResponse head = mantaClient.head(path);
        byte[] remoteMd5 = head.getMd5Bytes();

        assertTrue(Arrays.equals(remoteMd5, expectedMd5),
                "MD5 values do not match");

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Concatenating {} parts took {} seconds",
                parts.length, totalCompletionTime.toMillis() / 1000);
    }

    public void canAbortMultipartBinary() throws IOException {
        final long oneMB = 1024L * 1024L;

        File[] parts = new File[] {
                createTemporaryDataFile(oneMB, 1),
                createTemporaryDataFile(oneMB, 1),
                createTemporaryDataFile(oneMB, 1)
        };

        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path);

        for (int i = 0; i < parts.length; i++) {
            File part = parts[i];
            int partNumber = i + 1;
            multipart.putPart(uploadId, partNumber, part);
        }

        multipart.validateThereAreNoMissingParts(uploadId);
        multipart.complete(uploadId);

        Instant start = Instant.now();
        multipart.abort(uploadId);
        multipart.waitForCompletion(uploadId);
        Instant end = Instant.now();

        assertTrue(multipart.isComplete(uploadId));
        assertFalse(multipart.isStarted(uploadId));

        MantaJob job = multipart.findJob(uploadId);

        if (job.getCancelled()) {
            if (!mantaClient.existsAndIsAccessible(path)) {
                throw new SkipException("File was actually created. Actual job state is: "
                        + job.toString());
            }
        } else {
            fail("Job wasn't cancelled:" + job.toString());
        }

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Aborting took {} seconds",
                totalCompletionTime.toMillis() / 1000);
    }

    private File createTemporaryDataFile(final long sizeInBytes, final int partNumber)
            throws IOException {
        File temp = File.createTempFile(String.format("multipart-%d", partNumber), ".data");
        temp.deleteOnExit();

        try (OutputStream out = new FileOutputStream(temp);
             InputStream in = new RandomInputStream(sizeInBytes)) {
            IOUtils.copy(in, out);
        }

        return temp;
    }

    private File concatenateFiles(final File... files) throws IOException{
        File temp = File.createTempFile("multipart-concatenated", ".data");
        temp.deleteOnExit();

        for (File file : files) {
            if (temp.exists()) {
                try (OutputStream out = new FileOutputStream(temp, true);
                     InputStream in = new FileInputStream(file)) {
                    IOUtils.copy(in, out);
                }
            } else {
                FileUtils.copyFile(file, temp);
            }
        }

        return temp;
    }

    private byte[] md5(final File file) throws IOException {
        try (InputStream in = new FileInputStream(file)) {
            return DigestUtils.md5(in);
        }
    }
}
