/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test that verifies that exception class can handle JSON correctly.
 */
@Test
@SuppressWarnings("DeadException")
public class MantaClientHttpResponseExceptionTest {
    public void canHandleJson() throws Exception {
        final String jsonResponse = "{ \"code\":\"InternalError\", \"message\":\"Unit test message.\"}";

        final HttpRequest request = request();
        final HttpResponse response = responseWithErrorJson(jsonResponse);

        new MantaClientHttpResponseException(request, response,
                        "/user/stor/an/object");
    }

    public void canHandleJsonWithUnknownProperty() {
        final String jsonResponse = "{ \"id\":\"foo\", \"code\":\"InternalError\", \"message\":\"Unit test message.\"}";

        final HttpRequest request = request();
        final HttpResponse response = responseWithErrorJson(jsonResponse);

        new MantaClientHttpResponseException(request, response,
                "/user/stor/an/object");
    }

    public void errorContainsResponseHeaders() {
        final String jsonResponse = "{ \"code\":\"InternalError\", \"message\":\"Unit test message.\"}";

        final HttpRequest request = request();
        final HttpResponse response = responseWithErrorJson(jsonResponse);

        MantaClientHttpResponseException exception = new MantaClientHttpResponseException(request, response,
                "/user/stor/an/object");

        final MantaHttpHeaders headers = exception.getHeaders();
        Assert.assertNotNull(headers);
        Assert.assertNotNull(headers.getDate());
        Assert.assertNotNull(headers.get(HttpHeaders.SERVER));
        Assert.assertNotNull(headers.getRequestId());
        Assert.assertNotNull(headers.get("x-response-time"));
        Assert.assertNotNull(headers.get(HttpHeaders.CONNECTION));
    }

    private static HttpRequest request() {
        final HttpRequest request = mock(HttpRequest.class);
        when(request.getRequestLine()).thenReturn(new BasicRequestLine("GET", UNIT_TEST_URL,
                HttpVersion.HTTP_1_1));

        return request;
    }

    private static HttpResponse responseWithErrorJson(final String json) {
        final StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_SERVICE_UNAVAILABLE, "UNAVAILABLE");
        final HttpResponse response = mock(HttpResponse.class);
        when(response.getStatusLine()).thenReturn(statusLine);

        HttpEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
        when(response.getEntity()).thenReturn(entity);

        Header[] headers = new Header[] {
                new BasicHeader(HttpHeaders.DATE, "Wed, 13 Dec 2017 17:18:48 GMT"),
                new BasicHeader(HttpHeaders.SERVER, "Manta"),
                new BasicHeader(MantaHttpHeaders.REQUEST_ID, UUID.randomUUID().toString()),
                new BasicHeader("x-response-time", "198"),
                new BasicHeader(HttpHeaders.CONNECTION, "Keep-alive")
        };

        when(response.getAllHeaders()).thenReturn(headers);

        return response;
    }
}
