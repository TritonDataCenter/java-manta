/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.client.HttpHelper;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.domain.ErrorDetail;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Exception class representing a failure in the contract of Manta's behavior.
 * This exception class is thrown when an unexpected response code is returned
 * from Manta.
 */
public class MantaClientHttpResponseException extends MantaIOException {
    private static final long serialVersionUID = 5696045042485801788L;

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaClientHttpResponseException.class);

    /**
     * Server error code returned from Manta.
     */
    private MantaErrorCode serverCode;

    /**
     * Manta request id.
     */
    private String requestId;

    /**
     * Apache HTTP Client status response object giving us a code and phrase.
     */
    private StatusLine statusLine;

    /**
     * Headers associated with request.
     */
    private MantaHttpHeaders headers;

    /**
     * Content of HTTP response.
     */
    private String content;

    /**
     * Constructs an {@code CloudApiIOException} with {@code null}
     * as its error detail message.
     */
    public MantaClientHttpResponseException() {
    }

    /**
     * Constructs an {@code CloudApiIOException} with the specified detail message.
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     */
    public MantaClientHttpResponseException(final String message) {
        super(message);
    }

    /**
     * Constructs an {@code CloudApiIOException} with the specified detail message
     * and cause.
     * <p>
     * <p> Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.
     *
     * @param message The detail message (which is saved for later retrieval
     *                by the {@link #getMessage()} method)
     * @param cause   The cause (which is saved for later retrieval by the
     *                {@link #getCause()} method).  (A null value is permitted,
     */
    public MantaClientHttpResponseException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs an {@code CloudApiIOException} with the specified cause and a
     * detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of {@code cause}).
     * This constructor is useful for IO exceptions that are little more
     * than wrappers for other throwables.
     *
     * @param cause The cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A null value is permitted,
     *              and indicates that the cause is nonexistent or unknown.)
     */
    public MantaClientHttpResponseException(final Throwable cause) {
        super(cause);
    }

    /**
     * Builds a client exception object that is annotated with all of the
     * relevant request and response debug information.
     *
     * @param request HTTP request object
     * @param response HTTP response object
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     */
    public MantaClientHttpResponseException(final HttpRequest request,
                                            final HttpResponse response,
                                            final String path) {
        super(String.format("HTTP HEAD request failed to: %s", path));
        final HttpEntity entity = response.getEntity();
        final String jsonMimeType = ContentType.APPLICATION_JSON.getMimeType();

        final String mimeType;

        if (entity != null && entity.getContentType() != null) {
            mimeType = entity.getContentType().getValue();
        } else {
            mimeType = null;
        }

        ErrorDetail errorDetail = null;
        ObjectMapper mapper = MantaObjectMapper.INSTANCE;

        if (entity != null && (mimeType == null || mimeType.equals(jsonMimeType))) {
            try {
                try (InputStream json = entity.getContent()) {
                    errorDetail = mapper.readValue(json, ErrorDetail.class);
                } catch (RuntimeException e) {
                    LOGGER.warn("Unable to deserialize json error data", e);
                }
            } catch (IOException e) {
                LOGGER.warn("Problem getting response error content", e);
            }
        }

        StatusLine statusLine = response.getStatusLine();
        setRequestId(HttpHelper.extractRequestId(response));
        setStatusLine(statusLine);

        if (errorDetail == null) {
            setServerCode(MantaErrorCode.UNKNOWN_ERROR);
        } else {
            setServerCode(MantaErrorCode.valueOfCode(errorDetail.getCode()));
            setContextValue("server_message", errorDetail.getMessage());
        }

        HttpHelper.annotateContextedException(this, request, response);
    }

                                            /**
     * @return Whether received a successful HTTP status code {@code >= 200 && < 300} (see {@link #getStatusCode()}).
     */
    @Deprecated
    public final boolean isSuccessStatusCode() {
        final int code = getStatusCode();
        return code >= HttpStatus.SC_OK && code < HttpStatus.SC_BAD_REQUEST;
    }


    /**
     * @return The HTTP status code or {@code 0} for none.
     */
    public final int getStatusCode() {
        if (this.statusLine != null) {
            return statusLine.getStatusCode();
        } else {
            return 0;
        }
    }


    /**
     * @return The HTTP status message or {@code null} for none.
     */
    public final String getStatusMessage() {
        if (this.statusLine != null) {
            return statusLine.getReasonPhrase();
        } else {
            return null;
        }
    }


    /**
     * @return The HTTP response headers.
     */
    public final MantaHttpHeaders getHeaders() {
        return this.headers;
    }

    /**
     * Content returned as part of the HTTP response.
     *
     * @return HTTP body
     */
    public String getContent() {
        return this.content;
    }

    /**
     * Error code returned from server. This is a String constant defined by
     * Manta.
     * @return error code as an enum
     */
    public MantaErrorCode getServerCode() {
        return this.serverCode;
    }

    /**
     * The request id for the request as automatically assigned.
     *
     * @return uuid as string
     */
    public String getRequestId() {
        return requestId;
    }

    /**
     * Sets the request id that uniquely identifies HTTP request to Manta.
     *
     * @param requestId UUID as a string
     * @return the current instance of {@link MantaClientHttpResponseException}
     */
    public MantaClientHttpResponseException setRequestId(final String requestId) {
        this.requestId = requestId;
        setContextValue("requestId", requestId);
        return this;
    }

    /**
     * Sets the {@link StatusLine} returned from the HTTP request associated
     * with this exception.
     *
     * @param statusLine Apache HTTP Client status line object
     * @return the current instance of {@link MantaClientHttpResponseException}
     */
    public MantaClientHttpResponseException setStatusLine(final StatusLine statusLine) {
        this.statusLine = statusLine;
        setContextValue("statusLine", statusLine);
        return this;
    }

    /**
     * Sets the headers used to make the request that caused this exception.
     * @param headers Manta headers object
     *
     * @return the current instance of {@link MantaClientHttpResponseException}
     */
    public MantaClientHttpResponseException setHeaders(final MantaHttpHeaders headers) {
        this.headers = headers;
        setContextValue("headers", headers.toString());
        return this;
    }

    /**
     * Sets the content of the response that caused this exception.
     *
     * @param content free form text of associated content
     * @return the current instance of {@link MantaClientHttpResponseException}
     */
    public MantaClientHttpResponseException setContent(final String content) {
        this.content = content;
        setContextValue("content", content);
        return this;
    }

    /**
     * Sets the Manta server error code associated with this exception.
     *
     * @param serverCode enum of the server error code
     * @return the current instance of {@link MantaClientHttpResponseException}
     */
    public MantaClientHttpResponseException setServerCode(final MantaErrorCode serverCode) {
        this.serverCode = serverCode;
        setContextValue("serverCode", serverCode.getCode());
        return this;
    }
}
