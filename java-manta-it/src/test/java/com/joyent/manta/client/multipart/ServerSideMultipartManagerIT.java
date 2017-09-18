/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
@SuppressWarnings("Duplicates")
public class ServerSideMultipartManagerIT {
    private MantaClient mantaClient;
    private ServerSideMultipartManager multipart;

    private static final int FIVE_MB = 5242880;

    private String testPathPrefix;

    @BeforeClass
    public void beforeClass() throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();
        mantaClient = new MantaClient(config);

        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory()
            + MantaClient.SEPARATOR + "uploads")) {
            throw new SkipException("Server side uploads aren't supported in this Manta version");
        }

        multipart = new ServerSideMultipartManager(this.mantaClient);
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

    public void nonExistentFileHasNotStarted() throws IOException {
        String path = testPathPrefix + UUID.randomUUID().toString();
        UUID unknownId = new UUID(0L, -1L);
        ServerSideMultipartUpload upload = new ServerSideMultipartUpload(
                unknownId, path, null);
        assertEquals(multipart.getStatus(upload),
                MantaMultipartStatus.UNKNOWN);
    }

    public final void canAbortUpload() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        ServerSideMultipartUpload upload = multipart.initiateUpload(path);
        multipart.abort(upload);
        MantaMultipartStatus status = multipart.getStatus(upload);

        if (!status.equals(MantaMultipartStatus.ABORTED) && !status.equals(MantaMultipartStatus.ABORTING)) {
            Assert.fail("MPU is not in an aborted or aborting status. Actual status: " + status);
        }
    }

    public final void canListUploadsInProgress() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        ServerSideMultipartUpload upload = multipart.initiateUpload(path);

        try (Stream<MantaMultipartUpload> stream = multipart.listInProgress()) {
            Optional<MantaMultipartUpload> first = stream.filter(item ->
                    item.getId().equals(upload.getId())).findFirst();

            Assert.assertTrue(first.isPresent(), "Initiated upload wasn't present [upload=" + upload + "]");
        } finally {
            multipart.abort(upload);
        }
    }

    public final void canGetStatus() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(5242880);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path);
        MantaMultipartStatus newStatus = multipart.getStatus(upload);
        Assert.assertEquals(newStatus, MantaMultipartStatus.CREATED,
                            "Created status wasn't set. Actual status: " + newStatus);
        MantaMultipartUploadPart part = multipart.uploadPart(upload, 1, content);

        multipart.complete(upload, Stream.of(part));
        MantaMultipartStatus completeStatus = multipart.getStatus(upload);

        if (completeStatus.equals(MantaMultipartStatus.COMMITTING)) {
            MantaMultipartStatus lastStatus = completeStatus;
            for (int i = 2; i <= 15 ; i++) {
                try {
                    Thread.sleep((long)Math.pow(2, i));
                    if (mantaClient.existsAndIsAccessible(path)) {
                        lastStatus = null;
                        break;
                    }
                    lastStatus = multipart.getStatus(upload);
                } catch (InterruptedException ignored) {}
            }
            if (lastStatus != null) {
                Assert.fail("MPU " + upload  + "did not complete after multiple status checks.  Last status: " + lastStatus);
            }
        } else if (completeStatus.equals(MantaMultipartStatus.COMPLETED)) {
            // That was fast
            // NOTE: Server side MPU does not use this state yet
        } else {
            Assert.fail("MPU " + upload  + "in invalid status after complete invocation. Actual status: " + completeStatus);
        }
    }

    public final void canGetPart() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(5242880);

        String contentType = "application/rando; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        try {
            MantaMultipartUploadPart originalPart = multipart.uploadPart(upload, 1, content);
            MantaMultipartUploadPart pulledPart = multipart.getPart(upload, 1);

            Assert.assertEquals(pulledPart, originalPart,
                    "Part pulled from API isn't the same as the original");
        } finally {
            multipart.abort(upload);
        }
    }

    public final void canListParts() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(5242880);

        String contentType = "application/rando; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        try {
            MantaMultipartUploadPart originalPart = multipart.uploadPart(upload, 1, content);

            try (Stream<MantaMultipartUploadPart> parts = multipart.listParts(upload)) {
                Optional<MantaMultipartUploadPart> first = parts.findFirst();

                Assert.assertTrue(first.isPresent(), "First part wasn't listed");
                Assert.assertEquals(first.get(), originalPart,
                        "Original part is different from returned part");
            }
        } finally {
            multipart.abort(upload);
        }
    }

    public final void canUploadWithSinglePartByteArray() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(5242880);

        String contentType = "application/rando; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);
        headers.setDurabilityLevel(1);

        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-my-key-1", "my value 1");
        metadata.put("m-my-key-2", "my value 2");

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, metadata, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());

            // Headers
            Assert.assertEquals(in.getContentType(), contentType);
            Assert.assertEquals(in.getHeaderAsString(MantaHttpHeaders.HTTP_DURABILITY_LEVEL),
                    headers.getDurabilityLevel().toString());

            // Metadata
            Assert.assertEquals(in.getMetadata().get("m-my-key-1"), metadata.get("m-my-key-1"));
            Assert.assertEquals(in.getMetadata().get("m-my-key-2"), metadata.get("m-my-key-2"));
        }
    }

    public final void canUploadWithByteArrayAndMultipleParts() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(FIVE_MB + 1024);
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, FIVE_MB + 1024);

        String contentType = "application/something-never-seen-before; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content1);
        MantaMultipartUploadPart part2 = multipart.uploadPart(upload, 2, content2);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());

            Assert.assertEquals(in.getContentType(), contentType,
                    "Set content-type doesn't match actual content type");
        }
    }

    public void errorWhenMissingPart() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(FIVE_MB + 1024);
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);

        String contentType = "application/something-never-seen-before; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        multipart.uploadPart(upload, 2, content1);

        boolean thrown = false;

        try {
            multipart.validateThatThereAreSequentialPartNumbers(upload);
        } catch (MantaClientException e) {
            if ((int)e.getFirstContextValue("missing_part") == 1) {
                thrown = true;
            }
        }

        assertTrue(thrown, "Exception wasn't thrown");
    }

    public void cantUploadSmallMultipartString() throws IOException {
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

        final String name = "cant-upload-small-multipart-string-" + UUID.randomUUID();
        final String path = testPathPrefix + name;

        final ServerSideMultipartUpload upload = multipart.initiateUpload(path);
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            int partNumber = i + 1;
            MantaMultipartUploadTuple uploaded = multipart.uploadPart(
                    upload, partNumber, part);

            uploadedParts.add(uploaded);
        }

        multipart.validateThatThereAreSequentialPartNumbers(upload);

        boolean caught = false;
        try {
            multipart.complete(upload, uploadedParts);
        } catch (MantaMultipartException e) {
            Throwable cause = e.getCause();

            if (!(cause instanceof MantaClientHttpResponseException)) {
                Assert.fail("Unexpected exception cause", e);
            }

            MantaClientHttpResponseException responseException =
                    (MantaClientHttpResponseException)cause;
            Assert.assertEquals(responseException.getStatusCode(),
                    409,
                    "Unexpected response code");
            Assert.assertEquals(responseException.getServerCode(),
                    MantaErrorCode.MULTIPART_UPLOAD_INVALID_ARGUMENT,
                    "Unexpected error code");

            caught = true;

        }

        Assert.assertTrue(caught, "Expected exception was not caught");
    }
}
