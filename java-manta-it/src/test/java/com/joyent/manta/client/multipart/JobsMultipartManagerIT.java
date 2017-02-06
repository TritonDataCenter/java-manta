package com.joyent.manta.client.multipart;

import com.fasterxml.uuid.Generators;
import com.joyent.manta.benchmark.RandomInputStream;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.jobs.MantaJob;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
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
public class JobsMultipartManagerIT {
    private MantaClient mantaClient;
    private JobsMultipartManager multipart;
    private boolean usingEncryption;

    private String testPathPrefix;

    private Logger LOG = LoggerFactory.getLogger(getClass());

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {
        this.usingEncryption = BooleanUtils.toBoolean(usingEncryption);

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        this.mantaClient = new MantaClient(config);
//        if (usingEncryption) {
//            this.multipart = new EncryptingMantaMultipartManager(this.mantaClient);
//        } else {
                this.multipart = new JobsMultipartManager(this.mantaClient);
//        }
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, true);
    }


    @AfterClass
    public void afterClass() throws IOException {
        if (this.mantaClient != null) {
            // FIXME
            //this.mantaClient.deleteRecursive(testPathPrefix);
            this.mantaClient.closeWithWarning();
            this.multipart = null;
            this.mantaClient = null;
        }
    }

    public void nonExistentFileHasNotStarted() throws IOException {
        MantaMultipartUpload upload = new JobsMultipartUpload(new UUID(0L, -1L), "/dev/null");
        assertEquals(multipart.getStatus(upload),
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

        final MantaMultipartUpload upload = multipart.initiateUpload(path);
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(upload, partNumber,
                                                                      part);

            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);
        Instant start = Instant.now();
        multipart.complete(upload, uploadedParts);

        multipart.waitForCompletion(upload, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });
        Instant end = Instant.now();

        MantaMultipartStatus status = multipart.getStatus(upload);

        assertEquals(status, MantaMultipartStatus.COMPLETED);

        assertEquals(mantaClient.getAsString(path),
                combined.toString(),
                "Manta combined string doesn't match expectation: "
                     + multipart.findJob(upload));

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

        final String name = uploadName("will-run-function-when-waiting-too-long");
        final String path = testPathPrefix + name;

        final MantaMultipartUpload upload = multipart.initiateUpload(path);
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(upload, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);
        multipart.complete(upload, uploadedParts);

        Boolean flagChanged = multipart.waitForCompletion(upload,
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
        final MantaMultipartUpload upload = multipart.initiateUpload(path, null, headers);

        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(upload, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);
        multipart.complete(upload, uploadedParts);
        multipart.waitForCompletion(upload, (Function<UUID, Void>) uuid -> {
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
        if (usingEncryption) {
            metadata.put("e-hello", "world");
            metadata.put("e-foo", "bar");
        }

        final MantaMultipartUpload upload = multipart.initiateUpload(path, metadata);

        final ArrayList<MantaMultipartUploadPart> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadPart uploaded = multipart.uploadPart(upload, partNumber, part);
            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);
        multipart.complete(upload, uploadedParts);
        multipart.waitForCompletion(upload, (Function<UUID, Void>) uuid -> {
            fail("Completion operation didn't succeed within timeout");
            return null;
        });

        MantaMetadata remoteMetadata = mantaClient.head(path).getMetadata();


        assertEquals(remoteMetadata.keySet().stream().filter(key -> !key.startsWith("m-encrypt") && !key.startsWith("e-")).count(),
                     4, "Unexpected metadata size");
        assertTrue(remoteMetadata.containsKey(JobsMultipartManager.JOB_ID_METADATA_KEY));
        assertTrue(remoteMetadata.containsKey(JobsMultipartManager.UPLOAD_ID_METADATA_KEY));
        assertEquals(remoteMetadata.get("m-hello"), "world");
        assertEquals(remoteMetadata.get("m-foo"), "bar");
        if (usingEncryption) {
            assertEquals(remoteMetadata.get("e-hello"), "world");
            assertEquals(remoteMetadata.get("e-foo"), "bar");
        }
    }

    public void canUpload5MbX10MultipartBinary() throws IOException {
        canUploadMultipartBinary(5, 10);
    }

    public void canUpload5MbX200MultipartBinary() throws IOException {
        canUploadMultipartBinary(5, 200);
    }

    public void canUpload25MbX20MultipartBinary() throws IOException {
        canUploadMultipartBinary(5, 10);
    }

    private void canUploadMultipartBinary(final long sizeInMb,
                                          final int noOfParts) throws IOException {
        final long size = sizeInMb * 1024L * 1024L;

        File[] parts = new File[noOfParts];

        for (int i = 0; i < noOfParts; i++) {
            parts[i] = createTemporaryDataFile(size, 1);
        }

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

        // If we are using encryption the remote md5 is the md5 of the
        // cipher text.  To prove we uploaded the right bytes and can
        // get them back again, we need to download and calculate.
        if (usingEncryption) {
            MantaObjectInputStream gotObject = mantaClient.getAsInputStream(path);
            remoteMd5 = DigestUtils.md5(gotObject);
        }

        if (!Arrays.equals(remoteMd5, expectedMd5)) {
            StringBuilder builder = new StringBuilder();
            builder.append("MD5 values do not match - job id: ")
                   .append(multipart.findJob(upload));
            fail(builder.toString());
        }

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
        multipart.complete(upload, uploadedParts);

        Instant start = Instant.now();
        multipart.abort(upload);

        boolean caught = false;

        try {
            multipart.waitForCompletion(upload, (Function<UUID, Void>) uuid -> {
                fail("Completion operation didn't succeed within timeout");
                return null;
            });
        } catch (MantaMultipartException e) {
            if (e.getMessage().startsWith("Manta job backing multipart upload was "
                    + "aborted. This upload was unable to be completed.")) {
                caught = true;
            }
        }

        assertTrue(caught, "Backing job aborted exception wasn't thrown");
        Instant end = Instant.now();

        MantaMultipartStatus status = multipart.getStatus(upload);
        assertEquals(status, MantaMultipartStatus.ABORTED);

        MantaJob job = multipart.findJob(upload);

        Duration totalCompletionTime = Duration.between(start, end);

        LOG.info("Aborting took {} seconds",
                totalCompletionTime.toMillis() / 1000);

        if (!job.getCancelled()) {
            fail("Job wasn't cancelled:" + job.toString());
        }

        assertFalse(mantaClient.existsAndIsAccessible(multipart.multipartUploadDir(upload.getId())),
                "Upload directory shouldn't be present after abort");
    }

    public void canReturnEmptyMultipartList() throws IOException {
        List<MantaMultipartUpload> list = multipart.listInProgress().collect(Collectors.toList());
        if (!list.isEmpty()) {
            System.err.println("List should be empty. Actually had " + list.size() + " elements");
            list.forEach(element -> {
                System.err.println(element.getPath());
                try {
                    multipart.listParts(element).forEach(part -> {
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
