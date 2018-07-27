package com.joyent.manta.http;

import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaChecksumFailedException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaUnexpectedObjectTypeException;
import com.joyent.manta.http.entity.NoContentEntity;
import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.bouncycastle.crypto.Digest;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static com.joyent.manta.client.MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpStatus.SC_BAD_REQUEST;
import static org.apache.http.HttpStatus.SC_CREATED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Test
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

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);
        config = new StandardConfigContext().setMantaURL("http://localhost");
        reset(client, response, connCtx, statusLine);

        when(connCtx.getHttpClient())
                .thenReturn(client);

        when(client.execute(any()))
                .thenReturn(response);

        when(response.getStatusLine())
                .thenReturn(statusLine);

        when(response.getAllHeaders())
                .thenReturn(new Header[]{});
    }

    @AfterMethod
    public void teardown() throws Exception {
        Mockito.validateMockitoUsage();
    }

    public void testHttpPutValidatesResponseCodeSuccessfully() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        config.setVerifyUploads(false);
        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        final MantaObjectResponse put =
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null);

        Assert.assertNotNull(put);
    }

    public void testHttpPutValidatesResponseCodeAndThrowsWhenInvalid() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_OK);

        when(statusLine.getReasonPhrase())
                .thenReturn("OK");

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        Assert.assertThrows(MantaClientHttpResponseException.class, () ->
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null));
    }

    public void testHttpPutChecksumsSuccessfully() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        reset(client);
        when(client.execute(any()))
                .then((invocationOnMock) -> {
                    // checksum is calculated as entity is written to network
                    final HttpPut request = invocationOnMock.getArgument(0);
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

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        final MantaObjectResponse put =
                helper.httpPut("/path", null, new ByteArrayEntity(contentBytes), null);

        Assert.assertNotNull(put);
    }

    public void testHttpPutChecksumsCompareDifferentlyFails() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        reset(client);
        when(client.execute(any()))
                .then((invocationOnMock) -> {
                    // checksum is calculated as entity is written to network
                    final HttpPut request = invocationOnMock.getArgument(0);
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

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        Assert.assertThrows(MantaChecksumFailedException.class, () ->
                helper.httpPut("/path", null, new ByteArrayEntity(contentBytes), null));
    }

    public void testHttpPutThrowsWhenChecksumRequestedButNotReturned() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        Assert.assertThrows(MantaChecksumFailedException.class, () ->
                helper.httpPut("/path", null, NoContentEntity.INSTANCE, null));
    }

    public void throwsAppropriateExceptionWhenStreamingObjectThatIsDir() {
        final String path = "/user/stor/a-dir";
        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);
        final HttpGet get = helper.getRequestFactory().get(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Header[] responseHeaders = new Header[] {
                new BasicHeader(HttpHeaders.CONTENT_TYPE, DIRECTORY_RESPONSE_CONTENT_TYPE)
        };

        final BasicHttpEntity entity = new BasicHttpEntity();
        entity.setContentType(DIRECTORY_RESPONSE_CONTENT_TYPE);
        entity.setChunked(true);

        final EofSensorInputStream in = new EofSensorInputStream(
                new NullInputStream(0), null);
        entity.setContent(in);

        when(response.getEntity()).thenReturn(entity);
        when(response.getAllHeaders()).thenReturn(responseHeaders);

        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_OK);

        Assert.assertThrows(MantaUnexpectedObjectTypeException.class, () ->
                helper.httpRequestAsInputStream(get, headers));
    }

    @Test(expectedExceptions = MantaClientHttpResponseException.class,
          expectedExceptionsMessageRegExp = ".*code.*expected.*got.*" + SC_CREATED + ".*")
    public void deleteValidatesSuccessResponseCode() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(SC_CREATED); // this is a nonsensical response code for DELETE

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        helper.httpDelete("/path");
    }

    @Test(expectedExceptions = MantaClientHttpResponseException.class)
    public void deleteValidatesErrorResponseCode() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(SC_BAD_REQUEST);

        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);

        helper.httpDelete("/path");
    }


    public void deleteWithHeadersPassesAlongHeaders() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(SC_NO_CONTENT);


        final StandardHttpHelper helper = new StandardHttpHelper(connCtx, config);
        final MantaHttpHeaders ifMatchHeader = new MantaHttpHeaders();
        final String etag = "foo";
        ifMatchHeader.setIfMatch(etag);

        helper.httpDelete("/path", ifMatchHeader);

        verify(client).execute(argThat(r -> etag.equals(r.getFirstHeader(IF_MATCH).getValue())));
    }
}
