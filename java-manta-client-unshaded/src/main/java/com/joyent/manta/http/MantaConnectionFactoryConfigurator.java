/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.Validate;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Object for providing pre-configured HttpClient objects to use with {@link MantaConnectionFactory}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
public class MantaConnectionFactoryConfigurator {

    /**
     * An existing {@link HttpClientBuilder} to further configure.
     */
    private final HttpClientBuilder httpClientBuilder;

    /**
     * Packages together an externally-configured {@link HttpClientBuilder} and {@link HttpClientConnectionManager}
     * for use with the {@link com.joyent.manta.client.MantaClient} through a {@link MantaConnectionFactory}.
     *
     * @param httpClientBuilder the client builder
     */
    public MantaConnectionFactoryConfigurator(final HttpClientBuilder httpClientBuilder) {
        Validate.notNull(httpClientBuilder, "HttpClientBuilder must not be null");

        this.httpClientBuilder = httpClientBuilder;
    }

    HttpClientBuilder getHttpClientBuilder() {
        return this.httpClientBuilder;
    }
}
