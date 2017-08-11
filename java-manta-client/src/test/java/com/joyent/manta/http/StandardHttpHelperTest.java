package com.joyent.manta.http;

import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaChecksumFailedException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.entity.NoContentEntity;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.Whitebox;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by tomascelaya on 8/11/17.
 */
public class StandardHttpHelperTest {

    @Mock
    private final MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
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
        config = new StandardConfigContext().setMantaURL("");

        Whitebox.setInternalState(connectionFactory, "config", config);

        when(connCtx.getHttpClient())
                .thenReturn(client);

        when(client.execute(any()))
                .thenReturn(response);

        when(response.getStatusLine())
                .thenReturn(statusLine);

        when(response.getAllHeaders())
                .thenReturn(new Header[]{});

        when(connectionFactory.uriForPath(anyString()))
                .thenCallRealMethod();

        when(connectionFactory.put(Mockito.anyString()))
                .thenCallRealMethod();
    }

    @Test
    public void testHttpPutValidatesResponseCode() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_OK);

        when(statusLine.getReasonPhrase())
                .thenReturn("OK");

        StandardHttpHelper helper = new StandardHttpHelper(connCtx, connectionFactory, config);

        Assert.assertThrows(MantaClientHttpResponseException.class, () -> {
            helper.httpPut("/path", null, NoContentEntity.INSTANCE, null);
        });
    }


    @Test
    public void testHttpPutThrowsWhenChecksumRequestedButNotReturned() throws Exception {
        when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_NO_CONTENT);

        when(statusLine.getReasonPhrase())
                .thenReturn("No Content");

        StandardHttpHelper helper = new StandardHttpHelper(connCtx, connectionFactory, config);

        // it's the default but let's just be explicit
        config.setVerifyUploads(true);

        Assert.assertThrows(MantaChecksumFailedException.class, () -> {
            helper.httpPut("/path", null, NoContentEntity.INSTANCE, null);
        });
    }

}