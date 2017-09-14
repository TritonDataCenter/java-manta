package com.joyent.manta.exception;

import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaVersion;
import org.apache.http.*;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.DefaultHttpResponseFactory;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HTTP;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

/**
 * Created by tomascelaya on 6/28/17.
 */
public class MantaChecksumFailedExceptionTest {

    @Test
    public void containsExpectedFields() throws URISyntaxException {
        String dummyRequestId = UUID.randomUUID().toString();
        String dummyUri = "http://test.manta.io/account/file";

        HttpRequest request = new HttpPut(new URI(dummyUri));
        HttpResponse response = DefaultHttpResponseFactory.INSTANCE.newHttpResponse(
                new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, null), null);

        // requestId is extracted from the response, not the request
        response.setHeader(MantaHttpHeaders.REQUEST_ID, dummyRequestId);

        MantaChecksumFailedException e = new MantaChecksumFailedException("", request, response);

        Assert.assertEquals(e.getFirstContextValue("requestId"), dummyRequestId);
        Assert.assertEquals(e.getFirstContextValue("requestURL"), dummyUri);
        Assert.assertEquals(e.getFirstContextValue("responseStatusCode"), HttpStatus.SC_NO_CONTENT);

        String sdkVersion = e.getContextValues("mantaSdkVersion").get(0).toString();
        Assert.assertNotNull(sdkVersion,
                "SDK version data should be in exception context");
        Assert.assertEquals(sdkVersion, MantaVersion.VERSION,
                "SDK version should equal value coded");
    }



}
