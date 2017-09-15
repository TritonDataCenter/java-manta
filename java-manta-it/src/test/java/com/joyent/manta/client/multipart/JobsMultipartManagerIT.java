/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.fasterxml.uuid.Generators;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.jobs.MantaJob;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.test.util.RandomInputStream;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class JobsMultipartManagerIT {
    private MantaClient mantaClient;
    private JobsMultipartManager multipart;

    private String testPathPrefix;

    private Logger LOG = LoggerFactory.getLogger(getClass());

    @BeforeClass
    public void beforeClass() throws IOException {
        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();

        this.mantaClient = new MantaClient(config);

        this.multipart = new JobsMultipartManager(this.mantaClient);

        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);

        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (this.mantaClient != null) {
            this.mantaClient.deleteRecursive(testPathPrefix);
            this.mantaClient.closeWithWarning();
            this.multipart = null;
            this.mantaClient = null;
        }
    }

    public void nonExistentFileHasNotStarted() throws IOException {
        JobsMultipartUpload upload = new JobsMultipartUpload(new UUID(0L, -1L), "/dev/null");
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

        final JobsMultipartUpload upload = multipart.initiateUpload(path);
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

        final JobsMultipartUpload upload = multipart.initiateUpload(path);
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

        final String name = uploadName("can-store-content-type");
        final String path = testPathPrefix + name;

        final MantaHttpHeaders headers = new MantaHttpHeaders()
                .setContentType("text/plain");
        final JobsMultipartUpload upload = multipart.initiateUpload(path, null, headers);

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

        final JobsMultipartUpload upload = multipart.initiateUpload(path, metadata);

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
    }

    public void canUpload5MbX10MultipartBinary() throws IOException {
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

        final JobsMultipartUpload upload = multipart.initiateUpload(path);

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

        final JobsMultipartUpload upload = multipart.initiateUpload(path);

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
        final List<MantaMultipartUpload> list;

        try (Stream<MantaMultipartUpload> inProgress = multipart.listInProgress()) {
            list = inProgress.collect(Collectors.toList());
        }

        if (!list.isEmpty()) {
            System.err.println("List should be empty. Actually had " + list.size() + " elements");
            list.forEach(element -> {
                System.err.println(element.getPath());

                if (element instanceof JobsMultipartUpload) {
                    JobsMultipartUpload jobsUpload = (JobsMultipartUpload)element;
                    try (Stream<MantaMultipartUploadPart> innerStream = multipart.listParts(jobsUpload)) {
                        innerStream.forEach(part -> System.err.println("   " + part.getObjectPath()));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
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

        final List<MantaMultipartUpload> list;

        try (Stream<MantaMultipartUpload> inProgress = multipart.listInProgress()) {
             list = inProgress.collect(Collectors.toCollection(ArrayList::new));
        }

        try {
            assertFalse(list.isEmpty(), "List shouldn't be empty");

            for (MantaMultipartUpload upload : uploads) {
                assertTrue(list.contains(upload),
                        "Upload wasn't present in results: " + upload);
            }
        } finally {
            for (MantaMultipartUpload upload : uploads) {
                if (upload instanceof JobsMultipartUpload) {
                    multipart.abort((JobsMultipartUpload)upload);
                }
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
