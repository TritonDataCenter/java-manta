package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;

import java.util.Map;

/**
 * Object encapsulating the HTTP headers to be sent to the Manta API.
 * When non-standard HTTP headers are used as part of a PUT request to
 * Manta, they are stored as metadata about an object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaHttpHeaders extends HttpHeaders {
    /**
     * Creates an empty instance.
     */
    public MantaHttpHeaders() {
    }


    /**
     * Creates an instance with headers prepopulated from the specified {@link Map}.
     *
     * @param headers headers to prepopulate
     */
    public MantaHttpHeaders(final Map<? extends String, ?> headers) {
        putAll(headers);
    }


    /**
     * Creates an instance with headers prepopulated from the Google HTTP Client
     * headers class.
     *
     * @param headers headers to prepopulate
     */
    MantaHttpHeaders(HttpHeaders headers) {
        fromHttpHeaders(headers);
    }
}
