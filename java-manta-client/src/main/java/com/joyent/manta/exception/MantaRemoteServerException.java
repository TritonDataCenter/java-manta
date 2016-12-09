package com.joyent.manta.exception;

import org.apache.http.HttpResponse;

import static com.joyent.manta.http.MantaHttpHeaders.REQUEST_ID;

/**
 * Exception class indicating that there was a server error on a request to
 * the Manta and we were given a response without much debuggable
 * information.
 */
public class MantaRemoteServerException extends MantaIOException {

    private static final long serialVersionUID = -8536057311850512173L;

    /**
     * Creates a new instance.
     *
     * @param response the HTTP response from the server associated with the error
     */
    public MantaRemoteServerException(final HttpResponse response) {
        super(buildMessage(response));
        addContextValue("response", response);
        addContextValue("requestId", response.getFirstHeader(REQUEST_ID));
    }

    /**
     * Creates a new instance.
     *
     * @param response the HTTP response from the server associated with the error
     * @param cause exception to chain to this exception as a cause
     */
    public MantaRemoteServerException(final HttpResponse response, final Exception cause) {
        super(buildMessage(response), cause);
        addContextValue("response", response);
        addContextValue("requestId", response.getFirstHeader(REQUEST_ID));
    }

    /**
     * Generates an error message based on the contents of a HTTP response
     * object.
     *
     * @param response the HTTP response from the server associated with the error
     *
     * @return a generated error message
     */
    private static String buildMessage(final HttpResponse response) {
        return String.format("Remote server error [%d] - %s",
                response.getStatusLine().getStatusCode(),
                response.getStatusLine().getReasonPhrase());
    }
}
