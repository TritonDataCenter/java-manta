/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;

/**
 * @author Yunong Xiao Convenience wrapper over {@link HttpResponseException} so that consumers of this library don't
 *         have to depend on the underlying HTTP client implementation.
 */
public class MantaClientHttpResponseException extends MantaClientException {

    private static final long serialVersionUID = -7189613417468699L;

    /** The underlying {@link HttpResponseException} */
    private final HttpResponseException exception_;

    /**
     * 
     */
    public MantaClientHttpResponseException(HttpResponseException exception) {
        exception_ = exception;
    }

    /**
     * Returns whether received a successful HTTP status code {@code >= 200 && < 300} (see {@link #getStatusCode()}).
     * 
     * @since 1.7
     */
    public final boolean isSuccessStatusCode() {
        return exception_.isSuccessStatusCode();
    }

    /**
     * Returns the HTTP status code or {@code 0} for none.
     */
    public final int getStatusCode() {
        return exception_.getStatusCode();
    }

    /**
     * Returns the HTTP status message or {@code null} for none.
     */
    public final String getStatusMessage() {
        return exception_.getStatusMessage();
    }

    /**
     * Returns the HTTP response headers.
     */
    public HttpHeaders getHeaders() {
        return exception_.getHeaders();
    }
}