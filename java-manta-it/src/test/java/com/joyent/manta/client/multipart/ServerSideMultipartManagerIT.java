package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.KeyPairFactory;
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
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

import static org.testng.Assert.*;

@Test
public class ServerSideMultipartManagerIT {
    private MantaClient mantaClient;
    private ServerSideMultipartManager multipart;

    private static final int FIVE_MB = 5242880;
    private static final String TEST_DATA = "EMPIRE_IS_THE_BEST_EPISODE_EVER!";

    private String testPathPrefix;

    private Logger LOG = LoggerFactory.getLogger(getClass());

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);

        if (!mantaClient.existsAndIsAccessible(config.getMantaHomeDirectory()
            + MantaClient.SEPARATOR + "uploads")) {
            throw new SkipException("Server side uploads aren't supported in this Manta version");
        }

        final KeyPairFactory keyPairFactory = new KeyPairFactory(config);
        final KeyPair keyPair = keyPairFactory.createKeyPair();
        final MantaConnectionFactory connectionFactory = new MantaConnectionFactory(config, keyPair);
        final MantaConnectionContext connectionContext = new MantaApacheHttpClientContext(connectionFactory);

        // TODO: Add encryption manager when in use
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

    @Test
    public final void testUploadWithSinglePartByteArray() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = RandomUtils.nextBytes(5242880);

        String contentType = "text/plain; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 1, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        try (InputStream in = mantaClient.getAsInputStream(path);
             ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copy(in, out);

            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());
        }
    }

    @Test
    public final void testUploadWithByteArrayAndMultipleParts() throws IOException {
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
}