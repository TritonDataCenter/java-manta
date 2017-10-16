/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import com.joyent.manta.client.AuthenticationConfigurator;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

class DynamicHttpSignatureRequestInterceptor implements HttpRequestInterceptor {

    private final AuthenticationConfigurator authConfig;

    private HttpSignatureAuthScheme authScheme;

    DynamicHttpSignatureRequestInterceptor(final AuthenticationConfigurator authConfig) {
        this.authConfig = authConfig;
    }

    Credentials getCredentials() {
        return new UsernamePasswordCredentials(authConfig.getUsername(), null);
    }

    HttpSignatureAuthScheme getAuthScheme() {
        return new HttpSignatureAuthScheme(null, null);
    }

    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
        if (authConfig.authenticationDisabled()) {
            return;
        }

        final long start = System.nanoTime();
        final Header authorization = getAuthScheme().authenticate(getCredentials(), request, context);
        final long end = System.nanoTime();

        request.setHeader(authorization);
        request.setHeader("x-http-signing-time-ns", String.valueOf(end - start));
    }

}
