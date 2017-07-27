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
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.test.util.FailingInputStream;
import com.joyent.test.util.IndexedInterceptingCloseableHttpClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(groups = { "encrypted" })
@SuppressWarnings("Duplicates")
public class EncryptedServerSideMultipartManagerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(EncryptedServerSideMultipartManagerIT.class);

    private MantaClient mantaClient;
    private EncryptedServerSideMultipartManager multipart;

    private static final int FIVE_MB = 5242880;

    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private String testPathPrefix;

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@org.testng.annotations.Optional Boolean usingEncryption) throws IOException {
        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        if (!config.isClientEncryptionEnabled()) {
            throw new SkipException("Skipping tests if encryption is disabled");
        }
        mantaClient = new MantaClient(config);

        // sanity check
        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory())) {
            Assert.fail("Invalid credentials, cannot proceed with test suite");
        }

        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory()
                + MantaClient.SEPARATOR + "uploads")) {
            throw new SkipException("Server side uploads aren't supported in this Manta version");
        }

        multipart = new EncryptedServerSideMultipartManager(this.mantaClient);
        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s/",
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

    public void nonExistentFileHasNotStarted() throws IOException {
        String path = testPathPrefix + UUID.randomUUID().toString();
        UUID unknownId = new UUID(0L, -1L);

        ServerSideMultipartUpload wrapped = new ServerSideMultipartUpload(
                unknownId, path, null);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload =
                new EncryptedMultipartUpload<>(wrapped, null);
        assertEquals(multipart.getStatus(upload),
                MantaMultipartStatus.UNKNOWN);
    }

    public final void canAbortUpload() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        multipart.abort(upload);
        MantaMultipartStatus status = multipart.getStatus(upload);

        if (!status.equals(MantaMultipartStatus.ABORTED) && !status.equals(MantaMultipartStatus.ABORTING)) {
            Assert.fail("MPU is not in an aborted or aborting status. Actual status: " + status);
        }
    }

    public final void canListUploadsInProgress() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);

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

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
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

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
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

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
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

        // Add encrypted metadata
        metadata.put("e-my-key-3", "my value 3");
        metadata.put("e-my-key-4", "my value 4");

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, metadata, headers);
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

            try {
                // Metadata
                Assert.assertEquals(in.getMetadata().get("m-my-key-1"), metadata.get("m-my-key-1"));
                Assert.assertEquals(in.getMetadata().get("m-my-key-2"), metadata.get("m-my-key-2"));

                // Encrypted metadata
                Assert.assertEquals(in.getMetadata().get("e-my-key-3"), "my value 3");
                Assert.assertEquals(in.getMetadata().get("e-my-key-4"), "my value 4");
            } catch (AssertionError e) {
                System.err.println("Metadata contents:\n" + in.getMetadata());
                throw e;
            }
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

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
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

    public final void canUploadWithByteArrayAndMultipleFullParts() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(FIVE_MB *3);
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, FIVE_MB * 2 + 1);
        final byte[] content3 = Arrays.copyOfRange(content, FIVE_MB * 2 + 1, content.length);

        String contentType = "application/something-never-seen-before; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content1);
        MantaMultipartUploadPart part2 = multipart.uploadPart(upload, 2, content2);
        MantaMultipartUploadPart part3 = multipart.uploadPart(upload, 3, content3);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2, part3 };
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

    public final void canUploadWithSinglePartAndPerformRangeRequest() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(524);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(5L, 10L);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path, headers);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    Arrays.copyOfRange(content, 5, 11), out.toByteArray());
        }
    }

    public final void canUploadWithSinglePartAndPerformUnboundedRangeRequest() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(524);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setByteRange(5L, null);

        try (MantaObjectInputStream in = mantaClient.getAsInputStream(path, headers);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    Arrays.copyOfRange(content, 5, 524), out.toByteArray());
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

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path, null, headers);
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

        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        final ArrayList<MantaMultipartUploadTuple> uploadedParts =
                new ArrayList<>();

        Throwable exception = null;
        //0
        MantaMultipartUploadTuple uploaded0 = multipart.uploadPart(upload, 1, parts[0]);
        uploadedParts.add(uploaded0);
        //1-3
        for (int i = 1; i < parts.length; i++) {
            final int j = i;
            exception = Assert.expectThrows(MantaMultipartException.class,
                                        () -> {
                                            MantaMultipartUploadTuple uploaded = multipart.uploadPart(upload, j + 1, parts[j]);
                                            uploadedParts.add(uploaded);
                                        });
            assert exception.getCause() instanceof IllegalStateException;
        }
        Assert.assertEquals(uploadedParts.size(), 1, "only first small upload should succeed");

        // This "completes" but only uploads one part due to the previous exceptions
        multipart.complete(upload, uploadedParts);
        Assert.assertEquals(mantaClient.getAsString(path), "Hello ");
    }

    public final void doubleCompleteFails() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(524);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
        try (MantaObjectInputStream in =  mantaClient.getAsInputStream(path)) {
            Assert.assertEquals(IOUtils.toByteArray(in), content);
        }
        Assert.assertThrows(IllegalStateException.class,
                            () -> {
                                multipart.complete(upload, partsStream);
                            });
    }

    public final void properlyClosesStreamsAfterUpload() throws IOException {
        final URL testResource = Thread.currentThread().getContextClassLoader().getResource(TEST_FILENAME);
        Assert.assertNotNull(testResource, "Test file missing");

        final File uploadFile = File.createTempFile("upload", ".jpg");
        FileUtils.forceDeleteOnExit(uploadFile);
        FileUtils.copyURLToFile(testResource, uploadFile);
        Assert.assertTrue(0 < uploadFile.length(), "Error preparing upload file");

        final String path = testPathPrefix + UUID.randomUUID().toString();
        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);

        // we are not exercising the FileInputStream code-path due to issues with
        // spies failing the check for inputStream.getClass().equals(FileInputStream.class)
        InputStream contentStream = new FileInputStream(uploadFile);
        InputStream fileInputStreamSpy = Mockito.spy(contentStream);

        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, fileInputStreamSpy);

        // verify the stream we passed was closed
        Mockito.verify(fileInputStreamSpy, Mockito.times(1)).close();

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[]{part1};
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);
    }

    public final void canRetryUploadPart() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        int part3Size = RandomUtils.nextInt(500, 1500);
        LOGGER.info("Performing uploadPart retry test with part 3 size " + part3Size);
        final byte[] content = RandomUtils.nextBytes((2 * FIVE_MB) + part3Size);
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, (2 * FIVE_MB) + 1);
        final byte[] content3 = Arrays.copyOfRange(content, (2 * FIVE_MB) + 1, (2 * FIVE_MB) + part3Size);

        EncryptedMultipartUpload<ServerSideMultipartUpload> upload = multipart.initiateUpload(path);
        ArrayList<MantaMultipartUploadTuple> parts = new ArrayList<>(3);

        Assert.assertThrows(IOException.class, () -> {
            // partial read of content1
            InputStream content1BadInputStream = new FailingInputStream(new ByteArrayInputStream(content1), 1024);
            multipart.uploadPart(upload, 1, content1BadInputStream);
        });

        parts.add(multipart.uploadPart(upload, 1, content1));
        parts.add(multipart.uploadPart(upload, 2, content2));

        Assert.assertThrows(IOException.class, () -> {
            // smaller partial read of content3
            InputStream content3BadInputStream = new FailingInputStream(new ByteArrayInputStream(content3), 512);
            multipart.uploadPart(upload, 3, content3BadInputStream);
        });

        parts.add(multipart.uploadPart(upload, 3, content3));

        multipart.complete(upload, parts.stream());

        // auto-close of MantaEncryptedObjectInputStream validates authentication
        try (final MantaObjectInputStream in = mantaClient.getAsInputStream(path);
             final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Assert.assertTrue(in instanceof MantaEncryptedObjectInputStream);
            IOUtils.copy(in, out);
            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());
        }
    }

    public final void willNotAutomaticallyRetryOn503() throws IOException, IllegalAccessException {
        final EncryptedServerSideMultipartManager isolatedMultipart = new EncryptedServerSideMultipartManager(this.mantaClient);
        final String path = testPathPrefix + UUID.randomUUID().toString();

        // fields for reaching into multipart.wrapped.httpClient
        final Field FIELD_SERVERSIDEMULTIPARTMANAGER_CONNECTION_CONTEXT
                = FieldUtils.getField(ServerSideMultipartManager.class, "connectionContext", true);
        final Field FIELD_ENCRYPTEDMULTIPARTMANAGER_WRAPPED
                = FieldUtils.getField(EncryptedMultipartManager.class, "wrapped", true);
        final Field FIELD_MANTAAPACHEHTTPCLIENTCONTEXT_HTTP_CLIENT
                = FieldUtils.getField(MantaApacheHttpClientContext.class, "httpClient", true);

        final byte[] content = RandomUtils.nextBytes(FIVE_MB + RandomUtils.nextInt(500, 1500));
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, content.length);
        final ArrayList<MantaMultipartUploadTuple> parts = new ArrayList<>(2);

        final ServerSideMultipartManager wrappedManager =
                (ServerSideMultipartManager) FieldUtils.readField(FIELD_ENCRYPTEDMULTIPARTMANAGER_WRAPPED, isolatedMultipart, true);

        // swap the CloseableHttpClient with an intercepting wrapper
        final MantaConnectionContext connectionContext =
                (MantaConnectionContext) FieldUtils.readField(FIELD_SERVERSIDEMULTIPARTMANAGER_CONNECTION_CONTEXT, wrappedManager, true);
        final IndexedInterceptingCloseableHttpClient interceptor =
                new IndexedInterceptingCloseableHttpClient(connectionContext.getHttpClient());
        FieldUtils.writeField(FIELD_MANTAAPACHEHTTPCLIENTCONTEXT_HTTP_CLIENT, connectionContext, interceptor, true);

        // prepare a response for the interceptor to use
        final CloseableHttpResponse fakeFailureResponse = mock(CloseableHttpResponse.class);
        when(fakeFailureResponse.getStatusLine())
                .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_SERVICE_UNAVAILABLE, "Service Unavailable"));

        // only second request (first call to uploadPart below inside assertThrows) should fail
        interceptor.setResponse(1, fakeFailureResponse);

        // this is request 0
        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload = isolatedMultipart.initiateUpload(path);

        Assert.assertThrows(MantaMultipartException.class, () -> {
            // this response should be intercepted with fakeFailureResponse
            isolatedMultipart.uploadPart(upload, 1, content1);
        });

        parts.add(isolatedMultipart.uploadPart(upload, 1, content1));
        parts.add(isolatedMultipart.uploadPart(upload, 2, content2));

        isolatedMultipart.complete(upload, parts.stream());

        // auto-close of MantaEncryptedObjectInputStream validates authentication
        try (final MantaObjectInputStream in = mantaClient.getAsInputStream(path);
             final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Assert.assertTrue(in instanceof MantaEncryptedObjectInputStream);
            IOUtils.copy(in, out);
            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());
        }
    }
}
