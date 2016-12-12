/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.RequestIdInterceptor;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import static com.joyent.manta.client.MantaUtils.asString;
import static com.joyent.manta.http.MantaHttpHeaders.REQUEST_ID;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;
import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

/**
 * Helper class used for common HTTP operations against the Manta server.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class HttpHelper implements AutoCloseable {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpHelper.class);

    /**
     * Reference to the Apache HTTP Client HTTP request creation class.
     */
    private final MantaConnectionFactory connectionFactory;

    /**
     * Current connection context used for maintaining state between requests.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Object mapper for deserializing JSON.
     */
    private final ObjectMapper mapper = MantaObjectMapper.INSTANCE;

    /**
     * Creates a new instance of the helper class.
     *
     * @param connectionContext saved context used between requests to the Manta client
     * @param connectionFactory instance used for building requests to Manta
     */
    public HttpHelper(final MantaConnectionContext connectionContext,
                      final MantaConnectionFactory connectionFactory) {
        this.connectionContext = connectionContext;
        this.connectionFactory = connectionFactory;

    }

    /**
     * Executes a HTTP HEAD against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpHead(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("HEAD   {}", path);

        HttpHead head = connectionFactory.head(path);
        return executeAndCloseRequest(head, "HEAD   {} response [{}] {} ");
    }


    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpGet(final String path) throws IOException {
        return httpGet(path, null);
    }

    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers optional HTTP headers to include when getting an object
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpGet(final String path,
                                   final MantaHttpHeaders headers) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("GET    {}", path);

        final HttpGet get = connectionFactory.get(path);
        return executeAndCloseRequest(get, "GET    {} response [{}] {} ");
    }


    /**
     * Executes a HTTP DELETE against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Apache HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpDelete(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("DELETE {}", path);

        final HttpDelete delete = connectionFactory.delete(path);
        return executeAndCloseRequest(delete, "DELETE {} response [{}] {} ");
    }


    /**
     * Utility method for handling HTTP POST to the Apache HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    protected HttpResponse httpPost(final String path) throws IOException {
        return httpPost(path, null, null);
    }


    /**
     * Utility method for handling HTTP POST to the Apache HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @param entity content object to post
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    protected HttpResponse httpPost(final String path,
                                    final HttpEntity entity) throws IOException {
        return httpPost(path, null, entity);
    }


    /**
     * Utility method for handling HTTP POST to the Apache HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @param headers HTTP headers to attach to request
     * @param entity content object to post
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    protected HttpResponse httpPost(final String path,
                                    final MantaHttpHeaders headers,
                                    final HttpEntity entity) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("POST   {}", path);

        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        final HttpPost post = connectionFactory.post(path);
        post.setHeaders(httpHeaders.asApacheHttpHeaders());

        if (entity != null) {
            post.setEntity(entity);
        }

        CloseableHttpClient client = connectionContext.getHttpClient();

        try (CloseableHttpResponse response = client.execute(post)) {
            StatusLine statusLine = response.getStatusLine();
            LOGGER.debug("POST   {} response [{}] {} ", path, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());

            return response;
        }
    }


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
    protected MantaObjectResponse httpPut(final String path,
                                          final MantaHttpHeaders headers,
                                          final HttpEntity entity,
                                          final MantaMetadata metadata)
            throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("PUT    {}", path);

        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        if (metadata != null) {
            httpHeaders.putAll(metadata);
        }

        final HttpPut put = connectionFactory.put(path);
        put.setHeaders(httpHeaders.asApacheHttpHeaders());

        if (entity != null) {
            put.setEntity(entity);
        }

        CloseableHttpClient client = connectionContext.getHttpClient();

        try (CloseableHttpResponse response = client.execute(put)) {
            StatusLine statusLine = response.getStatusLine();
            LOGGER.debug("PUT    {} response [{}] {} ", path, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(response.getAllHeaders());
            // We add back in the metadata made in the request so that it is easily available
            responseHeaders.putAll(httpHeaders.metadata());

            MantaObjectResponse obj = new MantaObjectResponse(path, responseHeaders, metadata);

            if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                throw new MantaClientHttpResponseException(put, response,
                        put.getURI().getPath());
            }

            if (obj.getContentType() == null && entity != null && entity.getContentType() != null) {
                obj.setContentType(entity.getContentType().getValue());
            }

            return obj;
        }
    }


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
    protected CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                   final String logMessage,
                                                   final Object... logParameters)
            throws IOException {
        return executeRequest(request, (Integer)null, logMessage, logParameters);
    }

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
    protected CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                   final Integer expectedStatusCode,
                                                   final String logMessage,
                                                   final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext());
        StatusLine statusLine = response.getStatusLine();

        if (LOGGER.isDebugEnabled() && logMessage != null) {
            LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
        }

        if (isFailedStatusCode(expectedStatusCode, statusLine)) {
            throw new MantaClientHttpResponseException(request, response,
                    request.getURI().getPath());
        }

        return response;
    }

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
    protected CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                           final String logMessage,
                                                           final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, (Integer)null, logMessage, logParameters);
    }

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
    protected CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                           final Integer expectedStatusCode,
                                                           final String logMessage,
                                                           final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, true,
                logMessage, logParameters);
    }

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
    protected CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                   final Integer expectedStatusCode,
                                                   final boolean closeResponse,
                                                   final String logMessage,
                                                   final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();
        CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext());

        try {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled() && logMessage != null) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (isFailedStatusCode(expectedStatusCode, statusLine)) {
                String path = request.getURI().getPath();
                throw new MantaClientHttpResponseException(request, response,
                        path);
            }

            return response;
        } finally {
            if (closeResponse) {
                IOUtils.closeQuietly(response);
            }
        }
    }

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
    protected <R> R executeAndCloseRequest(final HttpUriRequest request,
                                           final Function<CloseableHttpResponse, R> responseAction,
                                           final String logMessage,
                                           final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, null,
                responseAction, logMessage, logParameters);
    }

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
    protected <R> R executeAndCloseRequest(final HttpUriRequest request,
                                           final Integer expectedStatusCode,
                                           final Function<CloseableHttpResponse, R> responseAction,
                                           final String logMessage,
                                           final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, responseAction,
                true, logMessage, logParameters);
    }

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
    protected <R> R executeRequest(final HttpUriRequest request,
                                   final Integer expectedStatusCode,
                                   final Function<CloseableHttpResponse, R> responseAction,
                                   final boolean closeResponse,
                                   final String logMessage,
                                   final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext());
        try {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (isFailedStatusCode(expectedStatusCode, statusLine)) {
                throw new MantaClientHttpResponseException(request, response,
                        request.getURI().getPath());
            }

            if (responseAction != null) {
                return responseAction.apply(response);
            } else {
                return null;
            }
        } finally {
            if (closeResponse) {
                IOUtils.closeQuietly(response);
            }
        }
    }

    /**
     * Utility method that determines if a request failed by comparing the
     * status code to an expectation if present (non-null) or by finding out
     * if the HTTP status code is less than 400.
     *
     * @param expectedStatusCode null for default behavior or the specific status code
     * @param statusLine status line object containing status code
     * @return boolean true indicates the request failed
     */
    private static boolean isFailedStatusCode(final Integer expectedStatusCode,
                                              final StatusLine statusLine) {
        int code = statusLine.getStatusCode();

        if (expectedStatusCode == null) {
            return code >= HttpStatus.SC_BAD_REQUEST;
        } else {
            return code != expectedStatusCode;
        }
    }

    /**
     * Extracts the request id from a {@link HttpRequest} object.
     *
     * @param response HTTP request object
     * @return UUID as a string representing unique request or null if not available
     */
    public static String extractRequestId(final HttpResponse response) {
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
    public static void annotateContextedException(final ExceptionContext exception,
                                                  final HttpRequest request,
                                                  final HttpResponse response) {
        Objects.requireNonNull(exception, "Exception context object must be present");

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
        }
    }

    @Override
    public void close() throws Exception {
        connectionContext.close();
    }
}
