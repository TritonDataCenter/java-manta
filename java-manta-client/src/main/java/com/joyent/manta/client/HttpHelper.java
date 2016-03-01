package com.joyent.manta.client;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.util.ObjectParser;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.SocketException;
import java.util.Objects;
import java.util.function.Function;

import static com.joyent.manta.client.MantaUtils.formatPath;

/**
 * Helper class used for common HTTP operations against the Manta server.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class HttpHelper {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HttpHelper.class);

    /**
     * Base Manta URL that all paths are appended to.
     */
    private final String url;

    /**
     * Reference to the Google HTTP Client HTTP request creation class.
     */
    private final HttpRequestFactory httpRequestFactory;


    /**
     * Creates a new instance of the helper class.
     *
     * @param url base Manta URL
     * @param httpRequestFactory request creation class
     */
    public HttpHelper(final String url,
                      final HttpRequestFactory httpRequestFactory) {
        this.url = url;
        this.httpRequestFactory = httpRequestFactory;
    }


    /**
     * Executes a HTTP HEAD against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Google HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpHead(final String path) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        LOG.debug("HEAD   {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequest request = httpRequestFactory.buildHeadRequest(genericUrl);

        final HttpResponse response;

        try {
            response = request.execute();
            LOG.debug("HEAD   {} response [{}] {} ", path, response.getStatusCode(),
                    response.getStatusMessage());
            return response;
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } catch (IOException | UncheckedIOException e) {
            final String requestId = MDC.get("mantaRequestId");
            final String msg = String.format("An IO problem happened when making "
                            + "a request (request: %s). Request details: %s", requestId,
                    request);

            throw new IOException(msg, e);
        }
    }


    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return Google HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpGet(final String path) throws IOException {
        return httpGet(path, null);
    }


    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param parser Parser used for parsing response into a POJO
     * @return Google HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpGet(final String path,
                                   final ObjectParser parser) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        return httpGet(genericUrl, parser);
    }


    /**
     * Executes a HTTP GET against the remote Manta API.
     *
     * @param genericUrl The URL to the object on Manta
     * @param parser Parser used for parsing response into a POJO
     * @return Google HTTP Client response object
     * @throws IOException when there is a problem getting the object over the network
     */
    protected HttpResponse httpGet(final GenericUrl genericUrl,
                                   final ObjectParser parser) throws IOException {
        Objects.requireNonNull(genericUrl, "URL must be present");

        LOG.debug("GET    {}", genericUrl.getRawPath());

        final HttpRequest request = httpRequestFactory.buildGetRequest(genericUrl);

        if (parser != null) {
            request.setParser(parser);
        }

        final HttpResponse response;

        try {
            response = request.execute();
            LOG.debug("GET    {} response [{}] {} ",
                    genericUrl.getRawPath(),
                    response.getStatusCode(),
                    response.getStatusMessage());
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } catch (IOException | UncheckedIOException e) {
            final String requestId = MDC.get("mantaRequestId");
            final String msg = String.format("An IO problem happened when making "
                    + "a request (request: %s). Request details: %s", requestId,
                    request);

            throw new IOException(msg, e);
        }

        return response;
    }


    /**
     * Utility method for handling HTTP POST to the Google HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @param content content object to post
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    protected HttpResponse httpPost(final String path,
                                    final HttpContent content) throws IOException {
        return httpPost(path, content, null);
    }


    /**
     * Utility method for handling HTTP POST to the Google HTTP Client.
     *
     * @param path path to post to (without hostname)
     * @param content content object to post
     * @param headers HTTP headers to attach to request
     * @return HTTP response object
     * @throws IOException thrown when there is a problem POSTing over the network
     */
    protected HttpResponse httpPost(final String path,
                                    final HttpContent content,
                                    final HttpHeaders headers) throws IOException {
        LOG.debug("POST   {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequest request = httpRequestFactory.buildPostRequest(genericUrl, content);

        if (content != null) {
            request.setContent(content);
        }

        if (headers != null) {
            request.setHeaders(headers);
        }

        return executeAndCloseRequest(request,
                "POST   {} response [{}] {} ", path);
    }


    /**
     * Executes an HTTP PUT against the remote Manta API.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers  optional HTTP headers to include when copying the object
     * @param content  Google HTTP Client content object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    protected MantaObjectResponse httpPut(final String path,
                                          final MantaHttpHeaders headers,
                                          final HttpContent content,
                                          final MantaMetadata metadata)
            throws IOException {
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        return httpPut(genericUrl, headers, content, metadata);
    }


    /**
     * Executes an HTTP PUT against the remote Manta API.
     *
     * @param genericUrl Full URL to the object on the Manta API
     * @param headers    optional HTTP headers to include when copying the object
     * @param content    Google HTTP Client content object
     * @param metadata   optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    protected MantaObjectResponse httpPut(final GenericUrl genericUrl,
                                          final MantaHttpHeaders headers,
                                          final HttpContent content,
                                          final MantaMetadata metadata)
            throws IOException {
        final String path = genericUrl.getRawPath();
        LOG.debug("PUT    {}", path);

        final MantaHttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new MantaHttpHeaders();
        } else {
            httpHeaders = headers;
        }

        if (metadata != null) {
            httpHeaders.putAll(metadata);
        }

        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, content);

        request.setHeaders(httpHeaders.asGoogleClientHttpHeaders());

        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug("PUT    {} response [{}] {} ", path, response.getStatusCode(),
                    response.getStatusMessage());
            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(response.getHeaders());
            // We add back in the metadata made in the request so that it is easily available
            responseHeaders.putAll(httpHeaders.metadata());

            return new MantaObjectResponse(path, responseHeaders, metadata);
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } catch (IOException | UncheckedIOException e) {
            final String requestId = MDC.get("mantaRequestId");
            final String msg = String.format("An IO problem happened when making "
                            + "a request (request: %s). Request details: %s", requestId,
                    request);

            throw new IOException(msg, e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
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
    protected HttpResponse executeAndCloseRequest(final HttpRequest request,
                                                  final String logMessage,
                                                  final Object... logParameters)
            throws IOException {
        HttpResponse response = null;

        try {
            response = request.execute();
            LOG.debug(logMessage, logParameters, response.getStatusCode(),
                    response.getStatusMessage());

            return response;
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } catch (IOException | UncheckedIOException e) {
            final String requestId = MDC.get("mantaRequestId");
            final String msg = String.format("An IO problem happened when making "
                            + "a request (request: %s). Request details: %s", requestId,
                    request);

            throw new IOException(msg, e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Executes a {@link HttpRequest}, logs the request and returns back the
     * response.
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
    protected <R> R executeAndCloseRequest(final HttpRequest request,
                                           final Function<HttpResponse, R> responseAction,
                                           final String logMessage,
                                           final Object... logParameters)
            throws IOException {
        HttpResponse response = null;

        try {
            response = request.execute();
            LOG.debug(logMessage, logParameters, response.getStatusCode(),
                    response.getStatusMessage());

            return responseAction.apply(response);
        } catch (final UncheckedIOException e) {
            final String requestId = MDC.get("mantaRequestId");
            final String msg = String.format("An IO problem happened when making "
                            + "a request (request: %s). Request details: %s", requestId,
                    request);

            throw new IOException(msg, e.getCause());
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }
}
