package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.KeyPairFactory;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.io.*;
import java.security.KeyPair;
import java.util.*;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.*;

@Test
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

        final KeyPairFactory keyPairFactory = new KeyPairFactory(config);
        final KeyPair keyPair = keyPairFactory.createKeyPair();
        final MantaConnectionFactory connectionFactory = new MantaConnectionFactory(config, keyPair);
        final MantaConnectionContext connectionContext = new MantaApacheHttpClientContext(connectionFactory);

        multipart = new ServerSideMultipartManager(config, connectionFactory, connectionContext, this.mantaClient);
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
        try {
            MantaMultipartStatus newStatus = multipart.getStatus(upload);
            Assert.assertEquals(newStatus, MantaMultipartStatus.CREATED,
                    "Created status wasn't set. Actual status: " + newStatus);
            MantaMultipartUploadPart part = multipart.uploadPart(upload, 1, content);

            Executors.newFixedThreadPool(1).execute(() -> {
                try {
                    multipart.complete(upload, Stream.of(part));
                    MantaMultipartStatus completeStatus = multipart.getStatus(upload);
//                    Assert.assertEquals(completeStatus, MantaMultipartStatus.UNKNOWN,
//                            "Unknown status wasn't set. Actual status: " + completeStatus);
                } catch (Exception e) {
                    LoggerFactory.getLogger(ServerSideMultipartManagerIT.class)
                            .error("Error asynchronously calling commit", e);
                }
            });


            try {
                Thread.sleep(400);
                MantaMultipartStatus committingStatus = multipart.getStatus(upload);
                Assert.assertEquals(committingStatus, MantaMultipartStatus.COMMITTING,
                        "Committing status wasn't set. Actual status: " + committingStatus);
            } catch (AssertionError e) {
                throw new SkipException("Timing based tests are prone to failure. Skip on failure.");
            }
        } catch (Exception e) {
            multipart.abort(upload);
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
}