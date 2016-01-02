/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.joyent.http.signature.google.httpclient.HttpSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import org.slf4j.MDC;

import java.io.IOException;

import static com.joyent.http.signature.HttpSignerUtils.X_REQUEST_ID_HEADER;

/**
 * Implementation of {@link HttpExecuteInterceptor} that performs HTTP signatures
 * on outgoing requests to the Manta API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class SigningInterceptor implements HttpExecuteInterceptor {
    /**
     * The time in which the last request was signed.
     */
    private long lastSigned = Long.MIN_VALUE;

    /**
     * The date of the HTTP header that was last signed.
     */
    private String lastDate;

    /**
     * The last HTTP signature value.
     */
    private String lastSignature;

    /**
     * Reference to configuration object.
     */
    private final ConfigContext config;

    /**
     * Reference to HTTP signing utility.
     */
    private final HttpSigner httpSigner;

    /**
     * Flag indicating that HTTP signature authentication is enabled.
     */
    private final boolean authEnabled;

    /**
     * Cache TTL.
     */
    private final int cacheTTL;

    /**
     * Creates a new instance of class.
     *
     * @param config configuration object
     * @param httpSigner HTTP signature generation object
     */
    public SigningInterceptor(final ConfigContext config, final HttpSigner httpSigner) {
        this.config = config;
        this.httpSigner = httpSigner;
        this.authEnabled = config.noAuth() == null || !config.noAuth();

        if (config.getSignatureCacheTTL() == null) {
            this.cacheTTL = DefaultsConfigContext.DEFAULT_SIGNATURE_CACHE_TTL;
        } else {
            this.cacheTTL = config.getSignatureCacheTTL();
        }
    }

    @Override
    public void intercept(final HttpRequest request) throws IOException {
        // Set timeouts
        final int httpTimeout;

        if (config.getTimeout() == null) {
            httpTimeout = DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT;
        } else {
            httpTimeout = config.getTimeout();
        }

        request.setReadTimeout(httpTimeout);
        request.setConnectTimeout(httpTimeout);

        // Load request ID into MDC so that it can be logged
        final Object requestId = request.getHeaders().get(X_REQUEST_ID_HEADER);
        if (requestId != null) {
            MDC.put("mantaRequestId", requestId.toString());
        }

        if (httpSigner == null || !authEnabled) {
            return;
        }

        // Sign request
        if (cacheTTL <= 0 || lastSigned - System.currentTimeMillis() > cacheTTL) {
            httpSigner.signRequest(request);
            lastSigned = System.currentTimeMillis();
            lastDate = request.getHeaders().getDate();
            lastSignature = request.getHeaders().getAuthorization();
        } else {
            request.getHeaders().setAuthorization(lastSignature);
            request.getHeaders().setDate(lastDate);
        }
    }
}
