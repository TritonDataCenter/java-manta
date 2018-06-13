package com.joyent.manta.http;

import org.apache.http.HttpResponse;
import org.apache.http.protocol.HttpRequestHandler;

interface EntityPopulatingHttpRequestHandler extends HttpRequestHandler {
    void populateEntity(final HttpResponse response);
}
