package com.joyent.manta.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.domain.ErrorDetail;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
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
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.InputStream;
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
     * Configuration context object that configuration values are pulled from.
     */
    private final ConfigContext config;

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
     * @param config Configuration context
     * @param connectionContext saved context used between requests to the Manta client
     * @param connectionFactory instance used for building requests to Manta
     */
    public HttpHelper(final ConfigContext config,
                      final MantaConnectionContext connectionContext,
                      final MantaConnectionFactory connectionFactory) {
        this.config = config;
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
                throw buildAnnotatedClientException(put, response,
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

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext());
        StatusLine statusLine = response.getStatusLine();

        if (LOGGER.isDebugEnabled() && logMessage != null) {
            LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
        }

        if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
            throw buildAnnotatedClientException(request, response,
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

        CloseableHttpClient client = connectionContext.getHttpClient();

        try (CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext())) {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled() && logMessage != null) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                throw buildAnnotatedClientException(request, response,
                        request.getURI().getPath());
            }

            return response;
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

        CloseableHttpClient client = connectionContext.getHttpClient();

        try (CloseableHttpResponse response = client.execute(request,
                connectionContext.getHttpContext())) {
            StatusLine statusLine = response.getStatusLine();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(logMessage, logParameters, statusLine.getStatusCode(),
                        statusLine.getReasonPhrase());
            }

            if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                throw buildAnnotatedClientException(request, response,
                        request.getURI().getPath());
            }

            return responseAction.apply(response);
        }
    }

    /**
     * Builds a client exception object that is annotated with all of the
     * relevant request and response debug information.
     * @param request HTTP request object
     * @param response HTTP response object
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return fully annotated exception instance
     */
    protected MantaClientHttpResponseException buildAnnotatedClientException(
            final HttpRequest request, final HttpResponse response,
            final String path) {
        final HttpEntity entity = response.getEntity();
        final String jsonContentType = ContentType.APPLICATION_JSON.toString();
        ErrorDetail errorDetail = null;

        if (entity != null && entity.getContentType().getValue().equals(jsonContentType)) {
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

        String msg = String.format("HTTP HEAD request failed to: %s", path);
        MantaClientHttpResponseException e = new MantaClientHttpResponseException(msg)
                .setRequestId(extractRequestId(request))
                .setStatusLine(statusLine);

        if (errorDetail == null) {
            e.setServerCode(MantaErrorCode.UNKNOWN_ERROR);
        } else {
            e.setServerCode(MantaErrorCode.valueOfCode(errorDetail.getCode()));
            e.setContextValue("server_message", errorDetail.getMessage());
        }

        annotateContextedException(e, request, response);
        return e;
    }

    /**
     * Extracts the request id from a {@link HttpRequest} object.
     *
     * @param request HTTP request object
     * @return UUID as a string representing unique request or null if not available
     */
    public static String extractRequestId(final HttpRequest request) {
        if (request == null) {
            return null;
        }

        final Header requestIdHeader = request.getFirstHeader(REQUEST_ID);
        final String requestId;

        if (requestIdHeader == null) {
            requestId = null;
        } else {
            requestId = requestIdHeader.getValue();
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
            final String requestId = extractRequestId(request);
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
