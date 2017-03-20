/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicRequestLine;
import org.apache.http.message.BasicStatusLine;
import org.testng.annotations.Test;

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

    private static HttpRequest request() {
        final HttpRequest request = mock(HttpRequest.class);
        when(request.getRequestLine()).thenReturn(new BasicRequestLine("GET", "http://localhost",
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

        return response;
    }
}
