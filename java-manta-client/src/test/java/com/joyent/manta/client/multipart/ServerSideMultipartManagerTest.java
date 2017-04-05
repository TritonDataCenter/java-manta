/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.BaseChainedConfigContext;
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
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

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
        final StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_SERVICE_UNAVAILABLE, "UNAVAILABLE");

        final String jsonResponse = "{ \"code\":\"InternalError\", \"message\":\"Unit test message.\"}";

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

    public void canAbortMpu() throws IOException {
        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_NO_CONTENT, "NO_CONTENT");
        ServerSideMultipartManager manager = buildMockManager(statusLine, null);

        UUID id = new UUID(0L, 0L);
        String partsDirectory = manager.uuidPrefixedPath(id);
        ServerSideMultipartUpload upload = new ServerSideMultipartUpload(
                id, null, partsDirectory);
        manager.abort(upload);
    }

    public void canCompleteMpu() throws IOException {
        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_CREATED, "NO_CONTENT");
        ServerSideMultipartManager manager = buildMockManager(statusLine, null);

        UUID id = new UUID(0L, 0L);
        String partsDirectory = manager.uuidPrefixedPath(id);
        ServerSideMultipartUpload upload = new ServerSideMultipartUpload(
                id, "/test/stor/myobject", partsDirectory);

        MantaMultipartUploadTuple[] unsortedTuples = new MantaMultipartUploadTuple[] {
                new MantaMultipartUploadTuple(5, new UUID(0L, 5L)),
                new MantaMultipartUploadTuple(3, new UUID(0L, 3L)),
                new MantaMultipartUploadTuple(1, new UUID(0L, 1L)),
                new MantaMultipartUploadTuple(2, new UUID(0L, 2L)),
                new MantaMultipartUploadTuple(4, new UUID(0L, 4L))
        };

        manager.complete(upload, Stream.of(unsortedTuples));
    }

    public void canCreateMpuRequestBodyJson() throws IOException {
        final ConfigContext config = mock(ConfigContext.class);
        final MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final MantaClient client = mock(MantaClient.class);
        final ServerSideMultipartManager manager = new ServerSideMultipartManager(
                config, connectionFactory, connectionContext, client);

        final String path = "/user/stor/object";

        final byte[] json = ServerSideMultipartManager.createMpuRequestBody(path, null, null);

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

        final byte[] json = ServerSideMultipartManager.createMpuRequestBody(path, null, headers);

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
            System.err.println(new String(json, StandardCharsets.UTF_8));
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

        final byte[] json = ServerSideMultipartManager.createMpuRequestBody(path, metadata, headers);

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
            System.err.println(new String(json, StandardCharsets.UTF_8));
            throw e;
        }
    }

    public void canCreateCommitRequestBody() throws IOException {
        MantaMultipartUploadTuple[] unsortedTuples = new MantaMultipartUploadTuple[] {
            new MantaMultipartUploadTuple(5, new UUID(0L, 5L)),
            new MantaMultipartUploadTuple(3, new UUID(0L, 3L)),
            new MantaMultipartUploadTuple(1, new UUID(0L, 1L)),
            new MantaMultipartUploadTuple(2, new UUID(0L, 2L)),
            new MantaMultipartUploadTuple(4, new UUID(0L, 4L))
        };

        Stream<MantaMultipartUploadTuple> partsStream = Arrays.stream(unsortedTuples);

        byte[] jsonRequest = ServerSideMultipartManager.createCommitRequestBody(partsStream).getLeft();

        ObjectNode objectNode = MantaObjectMapper.INSTANCE.readValue(jsonRequest, ObjectNode.class);
        @SuppressWarnings("unchecked")
        ArrayNode partsNode = (ArrayNode)objectNode.get("parts");

        // Verify that the parts are in the correct order
        try {
            Assert.assertEquals(partsNode.get(0).textValue(), unsortedTuples[2].getEtag());
            Assert.assertEquals(partsNode.get(1).textValue(), unsortedTuples[3].getEtag());
            Assert.assertEquals(partsNode.get(2).textValue(), unsortedTuples[1].getEtag());
            Assert.assertEquals(partsNode.get(3).textValue(), unsortedTuples[4].getEtag());
            Assert.assertEquals(partsNode.get(4).textValue(), unsortedTuples[0].getEtag());
        } catch (AssertionError e) {
            System.err.println(new String(jsonRequest, StandardCharsets.UTF_8));
            throw e;
        }
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void willFailToCreateCommitRequestBodyWhenThereAreNoParts() {
        ServerSideMultipartManager.createCommitRequestBody(Stream.empty());
    }

    // TEST UTILITY METHODS

    private SettableConfigContext<BaseChainedConfigContext> testConfigContext() {
        final String privateKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIEpQIBAAKCAQEA1lPONrT34W2VPlltA76E2JUX/8+Et7PiMiRNWAyrATLG7aRA\n" +
                "8iZ5A8o/aQMyexp+xgXoJIh18LmJ1iV8zqnr4TPXD2iPO92fyHWPu6P+qn0uw2Hu\n" +
                "ZZ0IvHHYED+fqxm7jz2ZjnfZl5Bz73ctjRF+77rPgOhhfv4KAc1d9CDsC+lHTqbp\n" +
                "ngufCYI4UWrnYoQ2JVXvEL9D5dMlHg0078qfh2cPg5xMOiOYobZeWqflV1Ue5I1Y\n" +
                "owNqiFzIDmBK0TKhnv+qQVNfMnNLJBYlYyGd0DUOJs8os5yivtuQXOhLZ0zLiTqK\n" +
                "JVjNJLzlcciqUf97Btm2enEHJ/khMFhrmoTQFQIDAQABAoIBAQCdc//grN4WHD0y\n" +
                "CtxNjd9mhVGWOsvTcTFRiN3RO609OiJuXubffmgU4rXm3dRuH67Wp2w9uop6iLO8\n" +
                "QNoJsUd6sGzkAvqHDm/eAo/PV9E1SrXaD83llJHgbvo+JZ+VQVhLCQQQZ/fQouyp\n" +
                "FbK/GgVY9LKQjydg9hw/6rGFMdJ3hFZVFqYFUhNpQKpczi6/lI/UIGcBhF3+8s/0\n" +
                "KMrz2PcCQFixlUFtBYXQHarOctxJDX7indchX08buwPqSv4YBBDLHUZkkMWomI/P\n" +
                "NjRDRyqnxvI03lHVfdbDzoPMxklJlHF68fkmp8NFLegnCBM8K0ae65Vk61b3oF9X\n" +
                "3eD6JtAZAoGBAPo/oBaJlA0GbQoJmULj6YqcQ2JKbUJtu7LP//8Gss47po4uqh6n\n" +
                "9vneKEpYYxuH5MXNsqtinmSQQMkE4UXoJSxJvnXNVAMQa3kUd0UgZSHjqWWgauDj\n" +
                "BjLQRpy9evef7VzTYx0xqEfAprsXxAoy0KXYN8gwgMC6MQgfZuFBgtxLAoGBANtA\n" +
                "1SVN/4wqrz4C8rpx7oZarHcMmGLiFF5OpKXlq1JY+U8IJ+WxMId3TI4h/h6OQGth\n" +
                "NJzQqFCS9H3a5EmqoNXHsLVXiKtG40+OzphSf9Y/NU7FtKanFWjfZl1ihhran1Fc\n" +
                "42jzN34EMM7Wm8p6HUK5qiDSCF+Ck0Lupud+WIkfAoGAXREOg3M0+UcbhDEfq23B\n" +
                "bAhDUymkyqCuvoh2hyzBkMtEXPpj0DTdN/3z8/o9GX8HiLzAJtbtWy7+uQO0l+AG\n" +
                "+xqN15e+F8mifowq8y1iDyFw3Ve0h+BGbN1idWZOdgsnJm+DG9dc4xp1p3zmLnjJ\n" +
                "efQYgr3vFD3qgD/Vbg6EEVMCgYEAnNfaIh+T6Y83YWL2hI2wFgiTS26FLGeSLoyP\n" +
                "l+WeEwB3CCRLdjK1BpM+/oYupWkZiDc3Td6uKUWXBNkrac9X0tZRAMinie7h+S2t\n" +
                "eKW7sWXyGnGv82+fDzCQp8ktKdSvF6MdQxyJ2+nfiHdZZxTIDc2HeIcHWlusQLs8\n" +
                "RmnJp/0CgYEA8AUV7K2KNRcwfuB1UjqhvlaqgiGixrItacGgnMQJ2cRSRSq2fZTm\n" +
                "eXxT9ugZ/9J9D4JTYZgdABnKvDjgbJMH9w8Nxr+kn/XZKNDzc1z0iJYwvyBOc1+e\n" +
                "prHvy4y+bCc0kLjCNQW4+/pVTWe1w8Mp63Vhdn+fO+wUGT3DTJGIXkU=\n" +
                "-----END RSA PRIVATE KEY-----";

        final String fingerprint = "ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02";

        StandardConfigContext settable = new StandardConfigContext();
        settable.setMantaUser("test");
        settable.setMantaKeyId(fingerprint);
        settable.setPrivateKeyContent(privateKey);
        settable.setEncryptionKeyId("unit-test-key");

        return new TestConfigContext(settable);
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
        final ThreadLocalSigner signer = new ThreadLocalSigner(new Signer.Builder(keyPair));

        final MantaConnectionFactory connectionFactory = new MantaConnectionFactory(config, keyPair, signer);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        when(response.getStatusLine()).thenReturn(statusLine);

        if (responseBody != null) {
            final BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(IOUtils.toInputStream(responseBody,
                    StandardCharsets.UTF_8));
            when(response.getEntity()).thenReturn(entity);
        }

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
