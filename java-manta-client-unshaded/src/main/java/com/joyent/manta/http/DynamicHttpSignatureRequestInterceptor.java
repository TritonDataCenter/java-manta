/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.http.signature.apache.httpclient.HttpSignatureAuthScheme;
import com.joyent.manta.config.AuthAwareConfigContext;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

/**
 * Request interceptor which can read potentially-changing authentication configuration from a
 * {@link AuthAwareConfigContext}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
class DynamicHttpSignatureRequestInterceptor implements HttpRequestInterceptor {

    /**
     * The auth context from which to read the {@link HttpSignatureAuthScheme} and
     * {@link org.apache.http.auth.Credentials}.
     */
    private final AuthAwareConfigContext authConfig;

    /**
     * Create an interceptor which will read authentication objects from a dynamic configuration.
     *
     * @param authConfig authentication context
     */
    DynamicHttpSignatureRequestInterceptor(final AuthAwareConfigContext authConfig) {
        this.authConfig = authConfig;
    }

    @Override
    public void process(final HttpRequest request, final HttpContext context)
            throws HttpException, IOException {
        if (authConfig.noAuth()) {
            return;
        }

        final long start = System.nanoTime();
        final HttpSignatureAuthScheme authScheme = authConfig.getAuthScheme();
        final Header authorization = authScheme.authenticate(authConfig.getCredentials(), request, context);
        final long end = System.nanoTime();

        request.setHeader(authorization);
        request.setHeader("x-http-signing-time-ns", String.valueOf(end - start));
    }

}
