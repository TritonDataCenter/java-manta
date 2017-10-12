/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.util.HttpClientBuilderCloner;
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
     * Unfortunately we need a cloner since {@link HttpClientBuilder} is a big mutable bucket of
     * settings we need to isolate changes to the initially-provided builder from modifications
     * we make ourselves. Unless we perform a "deep" clone modifications from one client instance
     * can persist. In reality we don't need to deep-clone anything other than the interceptor lists
     * since those are the only fields modified by {@link MantaConnectionFactory} when a custom
     * {@link HttpClientBuilder} is supplied using this class.
     * For example:
     *
     * <ol>
     * <li>{@link HttpClientBuilder} with custom interceptors
     * provided to {@link com.joyent.manta.client.LazyMantaClient}</li>
     *
     * <li>Request is made, {@link HttpHelper} instantiated with
     * {@link com.joyent.http.signature.apache.httpclient.HttpSignatureRequestInterceptor}</li>
     *
     * <li>Request fails because of invalid key, {@link com.joyent.manta.config.ConfigContext}
     * updated to use valid key</li>
     *
     * <li>User triggers client reload and makes a new request.</li>
     *
     * <li>Signature interceptor from initial instantiation is kept in addition to new
     * signature interceptor configured with the correct key.</li>
     * </ol>
     *
     * {@see LazyMantaClientTest#canReloadHttpHelperRepeatedlyWithoutLeakingInterceptors}
     */
    private static final HttpClientBuilderCloner CLONER = new HttpClientBuilderCloner();

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
        return CLONER.createClone(this.httpClientBuilder);
    }
}
