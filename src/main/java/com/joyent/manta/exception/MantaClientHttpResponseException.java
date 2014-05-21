/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;

/**
 * Convenience wrapper over {@link HttpResponseException} so that consumers of this library don't have to depend on the
 * underlying HTTP client implementation.
 *
 * @author Yunong Xiao
 */
public class MantaClientHttpResponseException extends MantaClientException {

    private static final long serialVersionUID = -7189613417468699L;

    /**
     * The underlying {@link HttpResponseException}.
     * */
    private final HttpResponseException exception_;

    /**
     *
     * @param exception The {@link HttpResponseException} to be wrapped.
     */
    public MantaClientHttpResponseException(final HttpResponseException exception) {
        this.exception_ = exception;
    }

    /**
     * @return Whether received a successful HTTP status code {@code >= 200 && < 300} (see {@link #getStatusCode()}).
     */
    public final boolean isSuccessStatusCode() {
        return this.exception_.isSuccessStatusCode();
    }

    /**
     * @return The HTTP status code or {@code 0} for none.
     */
    public final int getStatusCode() {
        return this.exception_.getStatusCode();
    }

    /**
     * @return The HTTP status message or {@code null} for none.
     */
    public final String getStatusMessage() {
        return this.exception_.getStatusMessage();
    }

    /**
     * @return The HTTP response headers.
     */
    public final HttpHeaders getHeaders() {
        return this.exception_.getHeaders();
    }
}
