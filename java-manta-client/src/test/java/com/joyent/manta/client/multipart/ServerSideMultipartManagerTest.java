package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.http.HttpHeaders;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Test
public class ServerSideMultipartManagerTest {
    private final ObjectMapper mapper = MantaObjectMapper.INSTANCE;

    public void canCreateMpuRequestBodyJson() throws IOException {
        final ServerSideMultipartManager manager = new ServerSideMultipartManager();
        final String path = "/user/stor/object";

        final byte[] json = manager.createMpuRequestBody(path, null, null);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
    }

    public void canCreateMpuRequestBodyJsonWithHeaders() throws IOException {
        final ServerSideMultipartManager manager = new ServerSideMultipartManager();
        final String path = "/user/stor/object";
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        headers.setDurabilityLevel(5);
        headers.setContentLength(423534L);
        headers.setContentMD5("e59ff97941044f85df5297e1c302d260");
        final Set<String> roles = new HashSet<>();
        roles.add("manta");
        roles.add("role2");
        headers.setRoles(roles);

        final byte[] json = manager.createMpuRequestBody(path, null, headers);

        ObjectNode jsonObject = mapper.readValue(json, ObjectNode.class);

        Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
        ObjectNode jsonHeaders = (ObjectNode) jsonObject.get("headers");

        Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_DURABILITY_LEVEL).asInt(),
                headers.getDurabilityLevel().intValue());
        Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_LENGTH).asLong(),
                headers.getContentLength().longValue());
        Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_MD5).textValue(),
                headers.getContentMD5());
        Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_ROLE_TAG).textValue(),
                headers.getFirstHeaderStringValue(MantaHttpHeaders.HTTP_ROLE_TAG));
    }

    public void canCreateMpuRequestBodyJsonWithHeadersAndMetadata() throws IOException {
        final ServerSideMultipartManager manager = new ServerSideMultipartManager();
        final String path = "/user/stor/object";
        final MantaHttpHeaders headers = new MantaHttpHeaders();

        headers.setDurabilityLevel(5);
        headers.setContentLength(423534L);
        headers.setContentMD5("e59ff97941044f85df5297e1c302d260");
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

        Assert.assertEquals(jsonObject.get("objectPath").textValue(), path);
        ObjectNode jsonHeaders = (ObjectNode)jsonObject.get("headers");

        Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_DURABILITY_LEVEL).asInt(),
                headers.getDurabilityLevel().intValue());
        Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_LENGTH).asLong(),
                headers.getContentLength().longValue());
        Assert.assertEquals(jsonHeaders.get(HttpHeaders.CONTENT_MD5).textValue(),
                headers.getContentMD5());
        Assert.assertEquals(jsonHeaders.get(MantaHttpHeaders.HTTP_ROLE_TAG).textValue(),
                headers.getFirstHeaderStringValue(MantaHttpHeaders.HTTP_ROLE_TAG));

        Assert.assertEquals(jsonHeaders.get("m-mykey1").textValue(),
                metadata.get("m-mykey1"));
        Assert.assertEquals(jsonHeaders.get("m-mykey2").textValue(),
                metadata.get("m-mykey2"));

        Assert.assertNull(jsonHeaders.get("e-mykey"), "Encrypted headers should not be included");
    }
}
