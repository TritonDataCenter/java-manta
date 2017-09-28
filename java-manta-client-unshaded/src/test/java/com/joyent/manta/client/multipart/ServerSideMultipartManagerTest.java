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
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SettableConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.FakeCloseableHttpClient;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.MantaHttpRequestFactory;
import com.joyent.manta.util.UnitTestConstants;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
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

    public void canUploadPartSuccess() throws IOException {
        final UUID uploadId = UUID.randomUUID();
        final String partsDirectory = "/test/uploads/a/abcdef";
        final String path = "/test/stor/object";
        final ServerSideMultipartUpload upload = new ServerSideMultipartUpload(uploadId, path, partsDirectory);
        final String etag = UUID.randomUUID().toString();

        final ServerSideMultipartManager mngr = buildMockManager(
                new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, "No Content"),
                "",
                (response) -> when(response.getFirstHeader(HttpHeaders.ETAG))
                        .thenReturn(new BasicHeader(HttpHeaders.ETAG, etag)));

        final MantaMultipartUploadPart part = mngr.uploadPart(upload, 1, new byte[0]);

        Assert.assertEquals(part.getObjectPath(), path);
        Assert.assertEquals(part.getPartNumber(), 1);
        Assert.assertEquals(part.getEtag(), etag);
    }

    public void canUploadPartValidatesResponseCode() throws IOException {
        final UUID uploadId = UUID.randomUUID();
        final String partsDirectory = "/test/uploads/a/abcdef";
        final String path = "/test/stor/object";
        final ServerSideMultipartUpload upload = new ServerSideMultipartUpload(uploadId, path, partsDirectory);

        final ServerSideMultipartManager mngr = buildMockManager(
                new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_SERVICE_UNAVAILABLE, "Service Unavailable"),
                String.format(
                        "{\"code\":\"%s\", \"message\":\"%s\"}",
                        MantaErrorCode.SERVICE_UNAVAILABLE_ERROR.getCode(),
                        "manta is unable to serve this request"));

        Assert.assertThrows(MantaMultipartException.class, () -> {
            mngr.uploadPart(upload, 1, new byte[0]);
        });
    }

    public void canUploadPartAndFailOnMissingEtag() throws IOException {
        final UUID uploadId = UUID.randomUUID();
        final String partsDirectory = "/test/uploads/a/abcdef";
        final String path = "/test/stor/object";
        final ServerSideMultipartUpload upload = new ServerSideMultipartUpload(uploadId, path, partsDirectory);

        // while it isn't strictly necessary to configure the mock to return null, it's better to be explicit about
        // what's being tested
        final ServerSideMultipartManager mngr = buildMockManager(
                new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, "No Content"),
                "",
                (response) -> when(response.getFirstHeader(HttpHeaders.ETAG)).thenReturn(null));

        final Exception e = Assert.expectThrows(MantaMultipartException.class, () -> {
            mngr.uploadPart(upload, 1, new byte[0]);
        });

        Assert.assertTrue(e.getMessage().contains("ETag missing from part response"));
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
        final String path = "/user/stor/object";

        final byte[] json = ServerSideMultipartManager.createMpuRequestBody(path, null, null);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
    }

    public void canCreateMpuRequestBodyJsonWithHeaders() throws IOException {
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

        StandardConfigContext settable = new StandardConfigContext();
        settable.setMantaUser("test");
        settable.setMantaKeyId(UnitTestConstants.FINGERPRINT);
        settable.setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY);
        settable.setEncryptionKeyId("unit-test-key");

        return new TestConfigContext(settable);
    }

    private ServerSideMultipartManager buildMockManager(final StatusLine statusLine,
                                                        final String responseBody)
            throws IOException {
        return buildMockManager(statusLine, responseBody, null);
    }

    private ServerSideMultipartManager buildMockManager(final StatusLine statusLine,
                                                        final String responseBody,
                                                        final Consumer<CloseableHttpResponse> responseConfigCallback)
            throws IOException {
        final ConfigContext config = testConfigContext();
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);

        final CloseableHttpResponse response = mock(CloseableHttpResponse.class);

        when(response.getStatusLine()).thenReturn(statusLine);

        if (responseConfigCallback != null) {
            responseConfigCallback.accept(response);
        }

        if (responseBody != null) {
            final BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(IOUtils.toInputStream(responseBody,
                    StandardCharsets.UTF_8));
            when(response.getEntity()).thenReturn(entity);
        }

        final CloseableHttpClient fakeClient = new FakeCloseableHttpClient(response);
        when(connectionContext.getHttpClient()).thenReturn(fakeClient);

        final HttpHelper httpHelper = mock(HttpHelper.class);
        when(httpHelper.getConnectionContext()).thenReturn(connectionContext);
        when(httpHelper.getRequestFactory()).thenReturn(new MantaHttpRequestFactory(config));

        final MantaClient client = mock(MantaClient.class);
        return new ServerSideMultipartManager(config, httpHelper, client);
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

        return (ServerSideMultipartUpload) upload;
    }
}
