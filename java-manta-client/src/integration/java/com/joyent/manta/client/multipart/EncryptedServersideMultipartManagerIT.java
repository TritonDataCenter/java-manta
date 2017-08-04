package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.EncryptionAuthenticationMode;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionContext;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import javax.crypto.SecretKey;

@Test
public class EncryptedServersideMultipartManagerIT {

    private static final Field FIELD_SERVERSIDEMULTIPARTMANAGER_CONNECTION_CONTEXT
            = FieldUtils.getField(ServerSideMultipartManager.class, "connectionContext", true);
    private static final Field FIELD_ENCRYPTEDMULTIPARTMANAGER_WRAPPED
            = FieldUtils.getField(EncryptedMultipartManager.class, "wrapped", true);
    private static final Field FIELD_MANTAAPACHEHTTPCLIENTCONTEXT_HTTP_CLIENT
            = FieldUtils.getField(MantaApacheHttpClientContext.class, "httpClient", true);

    private static final int FIVE_MB = 5242880;

    private MantaClient client;

    private String testPathPrefix;


    @BeforeClass()
    @Parameters({"usingEncryption", "encryptionCipher"})
    public void beforeClass(@Optional Boolean usingEncryption, @Optional String encryptionCipher)
            throws IOException {
        // Let TestNG configuration take precedence over environment variables
        TestConfigContext config = new TestConfigContext(new StandardConfigContext());

        if (ObjectUtils.firstNonNull(usingEncryption, false)) {
            SupportedCipherDetails cipherDetails =
                    SupportedCiphersLookupMap.INSTANCE.getOrDefault(encryptionCipher, DefaultsConfigContext.DEFAULT_CIPHER);

            config.setClientEncryptionEnabled(true);
            config.setEncryptionKeyId("integration-test-key");
            config.setEncryptionAuthenticationMode(EncryptionAuthenticationMode.Mandatory);

            config.setEncryptionAlgorithm(cipherDetails.getCipherId());
            SecretKey key = SecretKeyUtils.generate(cipherDetails);
            config.setEncryptionPrivateKeyBytes(key.getEncoded());
        }

        if (!config.isClientEncryptionEnabled()) {
            throw new SkipException("Skipping tests if encryption is disabled");
        }
        client = new MantaClient(config);

        // sanity check
        if (!client.existsAndIsAccessible(config.getMantaHomeDirectory())) {
            Assert.fail("Invalid credentials, cannot proceed with test suite");
        }

        if (!client.existsAndIsAccessible(config.getMantaHomeDirectory()
                + MantaClient.SEPARATOR + "uploads")) {
            throw new SkipException("Server side uploads aren't supported in this Manta version");
        }

        testPathPrefix = String.format("%s/stor/java-manta-integration-tests/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        client.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (client == null) {
            return;
        }

        client.deleteRecursive(testPathPrefix);
    }

    public void canRetryPartWithBadResponseCodeOnFirstPart() throws Exception {
        final String path = testPathPrefix + UUID.randomUUID().toString();

        final byte[] content = RandomUtils.nextBytes(FIVE_MB + RandomUtils.nextInt(500, 1500));
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, content.length);
        final ArrayList<MantaMultipartUploadTuple> parts = new ArrayList<>(2);

        final EncryptedServerSideMultipartManager manager = new EncryptedServerSideMultipartManager(client);
        final IndexedInterceptingCloseableHttpClient interceptor = wrapHttpClientWithInterceptor(manager);

        // prepare a response for the interceptor to use
        final CloseableHttpResponse fakeFailureResponse = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(fakeFailureResponse.getStatusLine())
                .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_SERVICE_UNAVAILABLE, "Service Unavailable"));

        interceptor.setResponse(1, fakeFailureResponse);

        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload = manager.initiateUpload(path);

        Assert.assertThrows(MantaMultipartException.class, () -> {
            // this response should be intercepted with fakeFailureResponse
            MantaMultipartUploadPart part = manager.uploadPart(upload, 1, content1);
            parts.add(part);
        });

        Assert.assertEquals(parts.size(), 0, "Parts should be empty since first part failed");

        parts.add(manager.uploadPart(upload, 1, content1));
        parts.add(manager.uploadPart(upload, 2, content2));

        manager.complete(upload, parts.stream());

        // auto-close of MantaEncryptedObjectInputStream validates authentication
        downloadAndValidateContent(path, content);

    }

    public void canRetryPartWithBadResponseCodeOnLastPart() throws Exception {
        final String path = testPathPrefix + UUID.randomUUID().toString();

        final byte[] content = RandomUtils.nextBytes(FIVE_MB + RandomUtils.nextInt(500, 1500));
        final byte[] content1 = Arrays.copyOfRange(content, 0, FIVE_MB + 1);
        final byte[] content2 = Arrays.copyOfRange(content, FIVE_MB + 1, content.length);
        final ArrayList<MantaMultipartUploadTuple> parts = new ArrayList<>(2);

        final EncryptedServerSideMultipartManager manager = new EncryptedServerSideMultipartManager(client);
        final IndexedInterceptingCloseableHttpClient interceptor = wrapHttpClientWithInterceptor(manager);

        // prepare a response for the interceptor to use
        final CloseableHttpResponse fakeFailureResponse = Mockito.mock(CloseableHttpResponse.class);
        Mockito.when(fakeFailureResponse.getStatusLine())
                .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_SERVICE_UNAVAILABLE, "Service Unavailable"));

        interceptor.setResponse(2, fakeFailureResponse);

        final EncryptedMultipartUpload<ServerSideMultipartUpload> upload = manager.initiateUpload(path);


        parts.add(manager.uploadPart(upload, 1, content1));

        Assert.assertThrows(MantaMultipartException.class, () -> {
            // this response should be intercepted with fakeFailureResponse
            MantaMultipartUploadPart part = manager.uploadPart(upload, 2, content2);
            parts.add(part);
        });
        Assert.assertEquals(parts.size(), 1, "Parts should only contain the first");

        parts.add(manager.uploadPart(upload, 2, content2));

        manager.complete(upload, parts.stream());

        // auto-close of MantaEncryptedObjectInputStream validates authentication
        downloadAndValidateContent(path, content);
    }

    // TEST UTILITY METHODS

    private void downloadAndValidateContent(String path, byte[] content) throws IOException {
        try (final MantaObjectInputStream in = client.getAsInputStream(path);
             final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            Assert.assertTrue(in instanceof MantaEncryptedObjectInputStream);
            IOUtils.copy(in, out);
            AssertJUnit.assertArrayEquals("Uploaded multipart data doesn't equal actual object data",
                    content, out.toByteArray());
        }
    }

    private IndexedInterceptingCloseableHttpClient wrapHttpClientWithInterceptor(EncryptedServerSideMultipartManager manager)
            throws IllegalAccessException {
        final ServerSideMultipartManager wrappedManager =
                (ServerSideMultipartManager) FieldUtils.readField(FIELD_ENCRYPTEDMULTIPARTMANAGER_WRAPPED, manager, true);
        final MantaConnectionContext connectionContext = (MantaConnectionContext)
                FieldUtils.readField(FIELD_SERVERSIDEMULTIPARTMANAGER_CONNECTION_CONTEXT, wrappedManager, true);
        final IndexedInterceptingCloseableHttpClient interceptor =
                new IndexedInterceptingCloseableHttpClient(connectionContext.getHttpClient());
        FieldUtils.writeField(FIELD_MANTAAPACHEHTTPCLIENTCONTEXT_HTTP_CLIENT, connectionContext, interceptor, true);
        return interceptor;
    }
}
