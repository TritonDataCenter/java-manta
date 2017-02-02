package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SettableConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.FakeCloseableHttpClient;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicStatusLine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test
public class ServerSideMultipartManagerTest {
    private final ObjectMapper mapper = MantaObjectMapper.INSTANCE;

    public void canInitiateUploadSuccess() throws IOException {
        final UUID uploadId = UUID.randomUUID();
        final String partsDirectory = "/test/uploads/a/abcdef";

        final StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_CREATED, "CREATED");

        final String jsonResponse = String.format("{ \"id\":\"%s\", \"partsDirectory\":\"%s\" }",
                uploadId.toString(), partsDirectory);

        String path = "/test/stor/object";

        ServerSideMultipartUpload serverSideUpload = initiateUploadWithAllParams(
                path, statusLine, jsonResponse);

        Assert.assertEquals(serverSideUpload.getId(), uploadId);
        Assert.assertEquals(serverSideUpload.getPath(), path);
        Assert.assertEquals(serverSideUpload.getPartsDirectory(), partsDirectory);
    }

    public void canInitiateUploadFailure503() throws IOException {
        final UUID uploadId = UUID.randomUUID();
        final String partsDirectory = "/test/uploads/a/abcdef";

        final StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_SERVICE_UNAVAILABLE, "UNAVAILABLE");

        final String jsonResponse = String.format("{ \"id\":\"%s\", \"partsDirectory\":\"%s\" }",
                uploadId.toString(), partsDirectory);

        String path = "/test/stor/object";

        boolean caught = false;
        try {
            initiateUploadWithAllParams(path, statusLine, jsonResponse);
        } catch (MantaMultipartException e) {
            if (e.getMessage().startsWith("Unable to create multipart upload")) {
                caught = true;
            } else {
                throw e;
            }
        }

        Assert.assertTrue(caught, "Expected exception was not caught");
    }

    public void canInitiateUploadFailureNonJsonResponse() throws IOException {
        final StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_CREATED, "CREATED");

        final String jsonResponse = "This is not JSON";

        String path = "/test/stor/object";

        boolean caught = false;
        try {
            initiateUploadWithAllParams(path, statusLine, jsonResponse);
        } catch (MantaMultipartException e) {
            if (e.getMessage().startsWith("Response body was not JSON")) {
                caught = true;
            } else {
                throw e;
            }
        }

        Assert.assertTrue(caught, "Expected exception was not caught");
    }

    public void canCreateMpuRequestBodyJson() throws IOException {
        final ConfigContext config = mock(ConfigContext.class);
        final MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final MantaClient client = mock(MantaClient.class);
        final ServerSideMultipartManager manager = new ServerSideMultipartManager(
                config, connectionFactory, connectionContext, client);

        final String path = "/user/stor/object";

        final byte[] json = manager.createMpuRequestBody(path, null, null);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
    }

    public void canCreateMpuRequestBodyJsonWithHeaders() throws IOException {
        final ConfigContext config = mock(ConfigContext.class);
        final MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final MantaClient client = mock(MantaClient.class);
        final ServerSideMultipartManager manager = new ServerSideMultipartManager(
                config, connectionFactory, connectionContext, client);

        final String path = "/user/stor/object";
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        headers.setDurabilityLevel(5);
        headers.setContentLength(423534L);
        headers.setContentMD5("e59ff97941044f85df5297e1c302d260");
        headers.setContentType(ContentType.APPLICATION_OCTET_STREAM.toString());
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
        headers.setRoles(roles);

        final byte[] json = manager.createMpuRequestBody(path, null, headers);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        try {
            Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
            ObjectNode jsonHeaders = (ObjectNode) jsonObject.get("headers");

            Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_DURABILITY_LEVEL.toLowerCase()).asInt(),
                    headers.getDurabilityLevel().intValue());
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_LENGTH.toLowerCase()).asLong(),
                    headers.getContentLength().longValue());
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_MD5.toLowerCase()).textValue(),
                    headers.getContentMD5());
            Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_ROLE_TAG.toLowerCase()).textValue(),
                    headers.getFirstHeaderStringValue(MantaHttpHeaders.HTTP_ROLE_TAG.toLowerCase()));
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_TYPE.toLowerCase()).textValue(),
                    "application/octet-stream");
        } catch (AssertionError e) {
            System.err.println(new String(json, "UTF-8"));
            throw e;
        }
    }

    public void canCreateMpuRequestBodyJsonWithHeadersAndMetadata() throws IOException {
        final ConfigContext config = mock(ConfigContext.class);
        final MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final MantaClient client = mock(MantaClient.class);
        final ServerSideMultipartManager manager = new ServerSideMultipartManager(
                config, connectionFactory, connectionContext, client);

        final String path = "/user/stor/object";
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        headers.setDurabilityLevel(5);
        headers.setContentLength(423534L);
        headers.setContentMD5("e59ff97941044f85df5297e1c302d260");
        headers.setContentType(ContentType.APPLICATION_OCTET_STREAM.toString());
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
        headers.setRoles(roles);

        final MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-mykey1", "key value 1");
        metadata.put("m-mykey2", "key value 2");
        metadata.put("e-mykey", "i should be ignored");

        final byte[] json = manager.createMpuRequestBody(path, metadata, headers);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        try {
            Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
            ObjectNode jsonHeaders = (ObjectNode) jsonObject.get("headers");

            Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_DURABILITY_LEVEL.toLowerCase()).asInt(),
                    headers.getDurabilityLevel().intValue());
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_LENGTH.toLowerCase()).asLong(),
                    headers.getContentLength().longValue());
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_MD5.toLowerCase()).textValue(),
                    headers.getContentMD5());
            Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_ROLE_TAG.toLowerCase()).textValue(),
                    headers.getFirstHeaderStringValue(MantaHttpHeaders.HTTP_ROLE_TAG.toLowerCase()));
            Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_TYPE.toLowerCase()).textValue(),
                    "application/octet-stream");

            Assert.assertEquals(jsonHeaders.get("m-mykey1").textValue(),
                    metadata.get("m-mykey1"));
            Assert.assertEquals(jsonHeaders.get("m-mykey2").textValue(),
                    metadata.get("m-mykey2"));

            Assert.assertNull(jsonHeaders.get("e-mykey"), "Encrypted headers should not be included");
        } catch (AssertionError e) {
            System.err.println(new String(json, "UTF-8"));
            throw e;
        }
    }

    private SettableConfigContext testConfigContext() {
        StandardConfigContext settable = new StandardConfigContext();
        settable.setMantaUser("test");

        TestConfigContext config = new TestConfigContext(settable);

        return config;
    }

    private ServerSideMultipartManager buildMockManager(final StatusLine statusLine,
                                                        final String responseBody)
            throws IOException {
        final ConfigContext config = testConfigContext();

        final KeyPair keyPair;
        try {
            keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new AssertionError(e);
        }

        final MantaConnectionFactory connectionFactory = new MantaConnectionFactory(config, keyPair);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        when(response.getStatusLine()).thenReturn(statusLine);
        final BasicHttpEntity entity = new BasicHttpEntity();

        entity.setContent(IOUtils.toInputStream(responseBody, "UTF-8"));

        when(response.getEntity()).thenReturn(entity);

        final CloseableHttpClient fakeClient = new FakeCloseableHttpClient(response);
        when(connectionContext.getHttpClient()).thenReturn(fakeClient);

        final MantaClient client = mock(MantaClient.class);
        return new ServerSideMultipartManager(
                config, connectionFactory, connectionContext, client);
    }

    private ServerSideMultipartUpload initiateUploadWithAllParams(final String path,
                                                                  final StatusLine statusLine,
                                                                  final String responseBody)
            throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        headers.setDurabilityLevel(5);
        headers.setContentLength(423534L);
        headers.setContentMD5("e59ff97941044f85df5297e1c302d260");
        headers.setContentType(ContentType.APPLICATION_OCTET_STREAM.toString());

        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
        headers.setRoles(roles);

        final MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-mykey1", "key value 1");
        metadata.put("m-mykey2", "key value 2");
        metadata.put("e-mykey", "i should be ignored");

        ServerSideMultipartManager manager = buildMockManager(statusLine, responseBody);
        MantaMultipartUpload upload = manager.initiateUpload(path, metadata, headers);

        Assert.assertEquals(upload.getClass(), ServerSideMultipartUpload.class);

        return (ServerSideMultipartUpload)upload;
    }
}
