/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.exception.MantaClientEncryptionException;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Function;

import static com.joyent.manta.http.MantaHttpHeaders.REQUEST_ID;
import static com.joyent.manta.util.MantaUtils.asString;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

/**
 * Interface describing the operations provided by our custom HTTP logic
 * that connects to the Manta server API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface HttpHelper extends AutoCloseable {
    /**
     * Executes a HTTP HEAD against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    HttpResponse httpHead(String path) throws IOException;

    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    HttpResponse httpGet(String path) throws IOException;

    /**
     * Executes a HTTP DELETE against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    HttpResponse httpDelete(String path) throws IOException;

    /**
     * Utility method for handling HTTP POST to the Apache HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    HttpResponse httpPost(String path) throws IOException;

    /**
     * Utility method for handling HTTP POST to the Apache HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @param headers HTTP headers to attach to request
     * @param entity content object to post
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    HttpResponse httpPost(String path,
                          MantaHttpHeaders headers,
                          HttpEntity entity) throws IOException;

    /**
     * <p>Returns the results of a HTTP request as an {@link InputStream}.</p>
     *
     * <p><strong>The underlying HttpResponse is not closed in this method.</strong></p>
     *
     * @param request The HTTP request object to be read as stream
     * @param headers optional HTTP headers to include when getting an object
     * @return {@link InputStream} that extends {@link MantaObjectResponse}.
     * @throws IOException when there is a problem getting the object over the network
     */
    MantaObjectInputStream httpRequestAsInputStream(
            HttpUriRequest request, MantaHttpHeaders headers) throws IOException;

    /**
     * Executes an HTTP PUT against the remote Manta API.
     *
     * @param path Full URL to the object on the Manta API
     * @param headers optional HTTP headers to include when copying the object
     * @param entity Apache HTTP Client content entity object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    MantaObjectResponse httpPut(String path,
                                MantaHttpHeaders headers,
                                HttpEntity entity,
                                MantaMetadata metadata) throws IOException;

    /**
     * Replaces the specified metadata to an existing Manta object using the
     * specified HTTP headers.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers HTTP headers to include when copying the object
     * @param metadata user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the metadata over the network
     */
    MantaObjectResponse httpPutMetadata(String path,
                                        MantaHttpHeaders headers,
                                        MantaMetadata metadata)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request and returns back the
     * response.
     *
     * @param request request object
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    CloseableHttpResponse executeRequest(HttpUriRequest request,
                                         String logMessage,
                                         Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request and returns back the
     * response.
     *
     * @param request request object
     * @param expectedStatusCode status code returned that indicates success
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    CloseableHttpResponse executeRequest(HttpUriRequest request,
                                         Integer expectedStatusCode,
                                         String logMessage,
                                         Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param request request object
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    CloseableHttpResponse executeAndCloseRequest(HttpUriRequest request,
                                                 String logMessage,
                                                 Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param request request object
     * @param expectedStatusCode status code returned that indicates success
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    CloseableHttpResponse executeAndCloseRequest(HttpUriRequest request,
                                                 Integer expectedStatusCode,
                                                 String logMessage,
                                                 Object... logParameters)
        throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param request request object
     * @param expectedStatusCode status code returned that indicates success
     * @param closeResponse when true we close the response before returning
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    CloseableHttpResponse executeRequest(HttpUriRequest request,
                                         Integer expectedStatusCode,
                                         boolean closeResponse,
                                         String logMessage,
                                         Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param <R> return value from responseAction function
     * @param request request object
     * @param responseAction action to perform against the response before it is closed
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    <R> R executeAndCloseRequest(HttpUriRequest request,
                                 Function<CloseableHttpResponse, R> responseAction,
                                 String logMessage,
                                 Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param <R> return value from responseAction function
     * @param request request object
     * @param expectedStatusCode status code returned that indicates success
     * @param responseAction action to perform against the response before it is closed
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    <R> R executeAndCloseRequest(HttpUriRequest request,
                                 Integer expectedStatusCode,
                                 Function<CloseableHttpResponse, R> responseAction,
                                 String logMessage,
                                 Object... logParameters)
            throws IOException;

    /**
     * Executes a {@link HttpRequest}, logs the request, closes the request and
     * returns back the response.
     *
     * @param <R> return value from responseAction function
     * @param request request object
     * @param expectedStatusCode status code returned that indicates success
     * @param responseAction action to perform against the response before it is closed
     * @param closeResponse when true we close the response before returning
     * @param logMessage log message associated with request that must contain
     *                   a substitution placeholder for status code and
     *                   status message
     * @param logParameters additional log placeholders
     * @return response object
     * @throws IOException thrown when we are unable to process the request on the network
     */
    <R> R executeRequest(HttpUriRequest request,
                         Integer expectedStatusCode,
                         Function<CloseableHttpResponse, R> responseAction,
                         boolean closeResponse,
                         String logMessage,
                         Object... logParameters)
            throws IOException;

    /**
     * Extracts the request id from a {@link HttpRequest} object.
     *
     * @param response HTTP request object
     * @return UUID as a string representing unique request or null if not available
     */
    static String extractRequestId(final HttpResponse response) {
        if (response == null) {
            return null;
        }

        final Header responseIdHeader = response.getFirstHeader(REQUEST_ID);
        final String requestId;


        if (responseIdHeader != null) {
            requestId = responseIdHeader.getValue();
        } else if (MDC.get(RequestIdInterceptor.MDC_REQUEST_ID_STRING) != null) {
            requestId = MDC.get(RequestIdInterceptor.MDC_REQUEST_ID_STRING);
        } else {
            requestId = null;
        }

        return requestId;
    }

    /**
     * Appends context attributes for the HTTP request and HTTP response objects
     * to a {@link ExceptionContext} instance.
     *
     * @param exception exception to append to
     * @param request HTTP request object
     * @param response HTTP response object
     */
    static void annotateContextedException(final ExceptionContext exception,
                                           final HttpRequest request,
                                           final HttpResponse response) {
        Validate.notNull(exception, "Exception context object must not be null");

        if (request != null) {
            final String requestId = extractRequestId(response);
            exception.setContextValue("requestId", requestId);

            final String requestDump = reflectionToString(request, SHORT_PREFIX_STYLE);
            exception.setContextValue("request", requestDump);
            exception.setContextValue("requestMethod", request.getRequestLine().getMethod());
            exception.setContextValue("requestURL", request.getRequestLine().getUri());
            final String requestHeaders = asString(request.getAllHeaders());
            exception.setContextValue("requestHeaders", requestHeaders);
            exception.setContextValue("loadBalancerAddress", MDC.get("mantaLoadBalancerAddress"));
        }

        if (response != null) {
            final String responseDump = reflectionToString(response, SHORT_PREFIX_STYLE);
            exception.setContextValue("response", responseDump);
            final String responseHeaders = asString(response.getAllHeaders());
            exception.setContextValue("responseHeaders", responseHeaders);

            final StatusLine statusLine = response.getStatusLine();

            if (statusLine != null) {
                exception.setContextValue("responseStatusCode", statusLine.getStatusCode());
                exception.setContextValue("responseStatusReason", statusLine.getReasonPhrase());
            }
        }
    }

    /**
     * Parses a response object for the unencrypted plaintext size in bytes.
     *
     * @param response response to parse
     * @param ciphertextSize size in bytes of the ciphertext
     * @param cipherDetails cipher used to decrypt
     * @return plaintext size in bytes
     * @throws MantaClientEncryptionException thrown when unable to get the plaintext size
     */
    static long attemptToFindPlaintextSize(final MantaObjectResponse response,
                                           final long ciphertextSize,
                                           final SupportedCipherDetails cipherDetails) {
        // If the calculation is accurate, then we attempt to calculate plaintext size
        if (!cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            return cipherDetails.plaintextSize(ciphertextSize);
        }

        // We try to get the metadata about the actual plaintext size
        String plaintextLengthHeaderVal = response.getHeaderAsString(
                MantaHttpHeaders.ENCRYPTION_PLAINTEXT_CONTENT_LENGTH);

        // If it is there, we replace the plaintext length specified with the file size
        if (plaintextLengthHeaderVal != null) {
            return Long.parseLong(plaintextLengthHeaderVal);
        }

        // Otherwise we error
        String msg = "Plaintext length specified is greater than "
                + "the size of the file and there is no reliable fallback "
                + "information for getting the real plaintext value";
        MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
        e.setContextValue("response", response);
        throw e;
    }
}
