package com.joyent.manta.client.multipart;

import com.fasterxml.uuid.Generators;
import com.joyent.manta.benchmark.RandomInputStream;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaHttpHeaders;
import com.joyent.manta.client.MantaJob;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
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
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class MantaMultipartManagerIT {
    private MantaClient mantaClient;
    private MantaMultipartManager multipart;

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
        this.multipart = new MantaMultipartManager(this.mantaClient);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (this.mantaClient != null) {
            this.mantaClient.deleteRecursive(testPathPrefix);
            this.mantaClient.closeWithWarning();
            this.multipart = null;
            this.mantaClient = null;
        }
    }

    public void nonExistentFileHasNotStarted() throws IOException {
        assertEquals(multipart.getStatus(new UUID(0L, -1L)),
                     MantaMultipartStatus.UNKNOWN);
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

        final String name = uploadName("can-upload-small-multipart-string");
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path).getId();
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(uploadId, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(uploadId);
        Instant start = Instant.now();
        multipart.complete(uploadId, uploadedParts);

        multipart.waitForCompletion(uploadId, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });
        Instant end = Instant.now();

        MantaMultipartStatus status = multipart.getStatus(uploadId);

        assertEquals(status, MantaMultipartStatus.COMPLETED);

        assertEquals(mantaClient.getAsString(path),
                combined.toString(),
                "Manta combined string doesn't match expectation: "
                        + multipart.findJob(uploadId));

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Concatenating {} parts took {} seconds",
                parts.length, totalCompletionTime.toMillis() / 1000);
    }

    public void willRunFunctionWhenWaitingTooLong() throws IOException {
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

        final String name = uploadName("will-run-function-when-waiting-too-long");
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path).getId();
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(uploadId, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(uploadId);
        multipart.complete(uploadId, uploadedParts);

        Boolean flagChanged = multipart.waitForCompletion(uploadId,
                Duration.ofNanos(0L), 1, uuid -> true);

        assertTrue(flagChanged, "wait timeout exceeded function was never run");
    }

    public void canStoreContentType() throws IOException {
        String[] parts = new String[] {
                "Hello ",
                "world ",
                "Joyent",
                "!"
        };

        final String name = uploadName("can-store-conten-type");
        final String path = testPathPrefix + name;

        final MantaHttpHeaders headers = new MantaHttpHeaders()
                .setContentType("text/plain");
        final UUID uploadId = multipart.initiateUpload(path, null, headers).getId();

        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(uploadId, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(uploadId);
        multipart.complete(uploadId, uploadedParts);
        multipart.waitForCompletion(uploadId, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });

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

        final String name = uploadName("can-store-metadata");
        final String path = testPathPrefix + name;

        final MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-hello", "world");
        metadata.put("m-foo", "bar");

        final UUID uploadId = multipart.initiateUpload(path, metadata).getId();

        final ArrayList<MantaMultipartUploadPart> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadPart uploaded = multipart.uploadPart(uploadId, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(uploadId);
        multipart.complete(uploadId, uploadedParts);
        multipart.waitForCompletion(uploadId, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });

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

        final String name = uploadName("can-upload-5mb-multipart-binary");
        final String path = testPathPrefix + name;

        final MantaMultipartUpload upload = multipart.initiateUpload(path);

        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            File part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(upload, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);
        Instant start = Instant.now();
        multipart.complete(upload, uploadedParts.stream());
        multipart.waitForCompletion(upload, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });
        Instant end = Instant.now();

        MantaMultipartStatus status = multipart.getStatus(upload);
        assertEquals(status, MantaMultipartStatus.COMPLETED);

        MantaObjectResponse head = mantaClient.head(path);
        byte[] remoteMd5 = head.getMd5Bytes();

        assertTrue(Arrays.equals(remoteMd5, expectedMd5),
                "MD5 values do not match - job id: " + multipart.findJob(upload.getId()));

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

        final String name = uploadName("can-abort-multipart-binary");
        final String path = testPathPrefix + name;

        final UUID uploadId = multipart.initiateUpload(path).getId();

        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            File part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(uploadId, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(uploadId);
        multipart.complete(uploadId, uploadedParts);

        Instant start = Instant.now();
        multipart.abort(uploadId);
        multipart.waitForCompletion(uploadId, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });
        Instant end = Instant.now();

        MantaMultipartStatus status = multipart.getStatus(uploadId);
        assertEquals(status, MantaMultipartStatus.ABORTED);

        MantaJob job = multipart.findJob(uploadId);

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Aborting took {} seconds",
                totalCompletionTime.toMillis() / 1000);

        if (!job.getCancelled()) {
            fail("Job wasn't cancelled:" + job.toString());
        }

        assertFalse(mantaClient.existsAndIsAccessible(multipart.multipartUploadDir(uploadId)),
                "Upload directory shouldn't be present after abort");
    }

    public void canReturnEmptyMultipartList() throws IOException {
        List<MantaMultipartUpload> list = multipart.listInProgress().collect(Collectors.toList());
        if (!list.isEmpty()) {
            System.err.println("List should be empty. Actually had " + list.size() + " elements");
            list.forEach(element -> {
                System.err.println(element.getPath());
                try {
                    multipart.listParts(element.getId()).forEach(part -> {
                        System.err.println("   " + part.getObjectPath());
                    });
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            throw new SkipException("List should be empty. Actually had " + list.size() + " elements");
        } else {
            assertTrue(true);
        }
    }

    public void canListMultipartUploadsInProgress() throws IOException {
        final String[] objects = new String[] {
                testPathPrefix + uploadName("can-list-multipart-uploads-in-progress-1"),
                testPathPrefix + uploadName("can-list-multipart-uploads-in-progress-2"),
                testPathPrefix + uploadName("can-list-multipart-uploads-in-progress-3")
        };

        final List<MantaMultipartUpload> uploads = new ArrayList<>(objects.length);

        for (String object: objects) {
            uploads.add(multipart.initiateUpload(object));
        }

        try {
            List<MantaMultipartUpload> list = multipart.listInProgress()
                    .collect(Collectors.toCollection(ArrayList::new));

            assertFalse(list.isEmpty(), "List shouldn't be empty");

            for (MantaMultipartUpload upload : uploads) {
                assertTrue(list.contains(upload),
                        "Upload wasn't present in results: " + upload);
            }
        } finally {
            for (MantaMultipartUpload upload : uploads) {
                multipart.abort(upload);
            }
        }
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

    private String uploadName(final String testName) {
        return String.format("%s-multipart-%s.data",
                testName,
                Generators.timeBasedGenerator().generate());
    }
}
