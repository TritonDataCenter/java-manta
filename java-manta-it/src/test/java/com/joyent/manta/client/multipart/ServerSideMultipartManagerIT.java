package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.KeyPairFactory;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.codec.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
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

    private static final String TEST_DATA = "EMPIRE_IS_THE_BEST_EPISODE_EVER!";

    private String testPathPrefix;

    private Logger LOG = LoggerFactory.getLogger(getClass());

    @BeforeClass()
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
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
    public final void testUploadWithByteArrayAndContentType() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content = TEST_DATA.getBytes(Charsets.UTF_8);

        String contentType = "text/plain; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 0, content);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        final String actual = mantaClient.getAsString(path, Charsets.UTF_8);
        Assert.assertEquals(actual, TEST_DATA);
    }

    @Test
    public final void testUploadWithByteArrayAndMultipleParts() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final byte[] content1 = getByteArrayOfLength(5100 * 1024);
        final byte[] content2 = getByteArrayOfLength(1024);

        String contentType = "text/plain; charset=UTF-8";
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType);

        ServerSideMultipartUpload upload = multipart.initiateUpload(path, null, headers);
        MantaMultipartUploadPart part1 = multipart.uploadPart(upload, 0, content1);
        MantaMultipartUploadPart part2 = multipart.uploadPart(upload, 1, content2);

        MantaMultipartUploadTuple[] parts = new MantaMultipartUploadTuple[] { part1, part2 };
        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(parts);
        multipart.complete(upload, partsStream);

        final String actual = mantaClient.getAsString(path, Charsets.UTF_8);
        Assert.assertTrue(actual.contains(TEST_DATA));
    }

    private final byte[] getByteArrayOfLength(int bytes) {
        double iterations = Math.ceil(bytes / TEST_DATA.length());
        String result = "";
        while (iterations > 0) {
            result.concat(TEST_DATA);
            iterations--;
        }

        return result.getBytes(Charsets.UTF_8);
    }
}
