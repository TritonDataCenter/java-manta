package com.joyent.manta.http;

import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaChecksumFailedException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.entity.NoContentEntity;
import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.bouncycastle.crypto.Digest;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

public class StandardHttpHelperTest {

    @Mock
    private final CloseableHttpClient client = mock(CloseableHttpClient.class);
    @Mock
    private final CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    @Mock
    private final MantaConnectionContext connCtx = mock(MantaConnectionContext.class);
    @Mock
    private final StatusLine statusLine = mock(StatusLine.class);

    private BaseChainedConfigContext config;

    private final MantaHttpRequestFactory requestFactory = new MantaHttpRequestFactory("");

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        config = new StandardConfigContext().setMantaURL("");

        when(connCtx.getHttpClient())
                .thenReturn(client);

        when(client.execute(any()))
                .thenReturn(response);

        when(response.getStatusLine())
                .thenReturn(statusLine);

        when(response.getAllHeaders())
                .thenReturn(new Header[]{});
    }

    @Test
    public void testHttpPutValidatesResponseCodeSuccessfully() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        config.setVerifyUploads(false);
        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, requestFactory, config);

        final MantaObjectResponse put =
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null);

        Assert.assertNotNull(put);
    }

    @Test
    public void testHttpPutValidatesResponseCodeAndThrowsWhenInvalid() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_OK);

        when(statusLine.getReasonPhrase())
                .thenReturn("OK");

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, requestFactory, config);

        Assert.assertThrows(MantaClientHttpResponseException.class, () ->
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null));
    }

    @Test
    public void testHttpPutChecksumsSuccessfully() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        reset(client);
        when(client.execute(any()))
                .then((invocationOnMock) -> {
                    // checksum is calculated as entity is written to network
                    final HttpPut request = invocationOnMock.getArgumentAt(0, HttpPut.class);
                    request.getEntity().writeTo(NullOutputStream.NULL_OUTPUT_STREAM);
                    return response;
                });

        final byte[] contentBytes = RandomUtils.nextBytes(100);
        final Digest digest = new FastMD5Digest();
        final byte[] checksumBytes = new byte[digest.getDigestSize()];
        digest.update(contentBytes, 0, contentBytes.length);
        digest.doFinal(checksumBytes, 0);

        when(response.getAllHeaders())
                .thenReturn(new Header[]{
                        new BasicHeader(
                                MantaHttpHeaders.COMPUTED_MD5,
                                Base64.getEncoder().encodeToString(checksumBytes))
                });

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, requestFactory, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        final MantaObjectResponse put =
                helper.httpPut("/path", null, new ByteArrayEntity(contentBytes), null);

        Assert.assertNotNull(put);
    }

    @Test
    public void testHttpPutChecksumsCompareDifferentlyFails() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        reset(client);
        when(client.execute(any()))
                .then((invocationOnMock) -> {
                    // checksum is calculated as entity is written to network
                    final HttpPut request = invocationOnMock.getArgumentAt(0, HttpPut.class);
                    request.getEntity().writeTo(NullOutputStream.NULL_OUTPUT_STREAM);
                    return response;
                });

        final byte[] contentBytes = StringUtils.repeat('a', 100).getBytes(StandardCharsets.UTF_8);

        when(response.getAllHeaders())
                .thenReturn(new Header[]{
                        new BasicHeader(
                                MantaHttpHeaders.COMPUTED_MD5,
                                "YmFzZTY0Cg==") // "base64" encoded in base64
                });

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, requestFactory, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        Assert.assertThrows(MantaChecksumFailedException.class, () ->
                helper.httpPut("/path", null, new ByteArrayEntity(contentBytes), null));
    }

    @Test
    public void testHttpPutThrowsWhenChecksumRequestedButNotReturned() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, requestFactory, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        Assert.assertThrows(MantaChecksumFailedException.class, () ->
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null));
    }
}