/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.domain.ObjectType;
import com.joyent.manta.exception.HttpDownloadContinuationException;
import com.joyent.manta.exception.MantaChecksumFailedException;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaObjectException;
import com.joyent.manta.exception.MantaUnexpectedObjectTypeException;
import com.joyent.manta.http.entity.DigestedEntity;
import com.joyent.manta.http.entity.NoContentEntity;
import com.joyent.manta.util.AutoContinuingInputStream;
import com.joyent.manta.util.InputStreamContinuator;
import com.joyent.manta.util.MantaUtils;
import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ProtocolException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.joyent.manta.config.DefaultsConfigContext.DOWNLOAD_CONTINUATIONS_DISABLED;
import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadRequestFingerprint;
import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadResponseFingerprint;
import static com.joyent.manta.http.HttpDownloadContinuationMarker.validateInitialExchange;
import static org.apache.http.HttpStatus.SC_ACCEPTED;
import static org.apache.http.HttpStatus.SC_NO_CONTENT;
import static org.apache.http.HttpStatus.SC_OK;

/**
 * Helper class used for common HTTP operations against the Manta server.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class StandardHttpHelper implements HttpHelper {

    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardHttpHelper.class);

    /**
     * Configuration to check for upload validation.
     */
    private final boolean verifyUploads;

    /**
     * Whether or not automatic download continuation is enabled.
     *
     * @see ApacheHttpGetResponseEntityContentContinuator
     * @see RetryConfigAware
     * @see HttpContextRetryCancellation
     */
    private final int maxDownloadContinuations;

    /**
     * Current connection context used for maintaining state between requests.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Factory used for request creation.
     */
    private final MantaHttpRequestFactory requestFactory;

    /**
     * Creates a new instance of the helper class.
     *
     * @param connectionContext saved context used between requests to the Manta client
     * @param connectionFactory ignored
     * @param config configuration context object
     */
    @Deprecated
    public StandardHttpHelper(final MantaConnectionContext connectionContext,
                              final MantaConnectionFactory connectionFactory,
                              final ConfigContext config) {
        this(connectionContext, config);
    }

    /**
     * Create a new instance of the HttpHelper which expects a static configuration since the request factory does not
     * have access to an {@link AuthAwareConfigContext}.
     *
     * Only used in testing.
     *
     * @param connectionContext connection object
     * @param config configuration context object
     */
    @SuppressWarnings("deprecation")
    StandardHttpHelper(final MantaConnectionContext connectionContext,
                       final ConfigContext config) {
        this(connectionContext, new MantaHttpRequestFactory(config.getMantaURL()), config);
    }

    /**
     * Creates a new instance of the helper class which can use a potentially-dynamic {@link MantaHttpRequestFactory}.
     *
     * @param connectionContext connection object
     * @param requestFactory instance used for building requests to Manta
     * @param config configuration context object
     */
    @Deprecated
    public StandardHttpHelper(final MantaConnectionContext connectionContext,
                              final MantaHttpRequestFactory requestFactory,
                              final ConfigContext config) {
        this(connectionContext,
             requestFactory,
             ObjectUtils.firstNonNull(
                     config.verifyUploads(),
                     DefaultsConfigContext.DEFAULT_VERIFY_UPLOADS),
             ObjectUtils.firstNonNull(
                     config.downloadContinuations(),
                     DefaultsConfigContext.DEFAULT_DOWNLOAD_CONTINUATIONS));
    }

    /**
     * Creates a new instance of the helper class which can use a potentially-dynamic {@link MantaHttpRequestFactory}
     * and knows whether or not it supports verifying uploads by calculating checksums and download resuming by retrying
     * requests with updated Range headers.
     *
     * @param connectionContext connection object
     * @param requestFactory instance used for building requests to Manta
     * @param verifyUploads whether or not to validate response checksums
     * @param downloadContinuation whether or not to return an auto-resuming {@link java.io.InputStream}
     */
    public StandardHttpHelper(final MantaConnectionContext connectionContext,
                              final MantaHttpRequestFactory requestFactory,
                              final boolean verifyUploads,
                              final Integer downloadContinuation) {
        this.connectionContext = Validate.notNull(connectionContext, "MantaConnectionContext must not be null");
        this.requestFactory = Validate.notNull(requestFactory, "MantaHttpRequestFactory must not be null");
        this.verifyUploads = verifyUploads;
        this.maxDownloadContinuations = validateDownloadContinuationConditions(connectionContext, downloadContinuation);

        // the following checks that:
        // 1. a value was provided for continuations
        // 2. it was not zero (i.e. explicitly disable continuations)
        // 3. we determined continuations should be disabled
        if (downloadContinuation != null
                && downloadContinuation != 0
                && this.maxDownloadContinuations == DOWNLOAD_CONTINUATIONS_DISABLED) {
            LOGGER.warn("Download continuation requested but provided connection context is invalid. Retries must "
                                + "be cancellable or disabled");
        }
    }

    @Override
    public MantaConnectionContext getConnectionContext() {
        return connectionContext;
    }

    @Override
    public MantaHttpRequestFactory getRequestFactory() {
        return requestFactory;
    }

    /**
     * @return true if we are validating MD5 checksums against the Manta Computed-MD5 header
     */
    @Deprecated
    protected boolean validateUploadsEnabled() {
        return this.verifyUploads;
    }

    @Override
    public HttpResponse httpHead(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("HEAD   {}", path);

        final HttpHead head = requestFactory.head(path);
        return executeAndCloseRequest(head, "HEAD   {} response [{}] {} ");
    }

    @Override
    public HttpResponse httpGet(final String path) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("GET    {}", path);

        final HttpGet get = requestFactory.get(path);
        return executeAndCloseRequest(get, "GET    {} response [{}] {} ");
    }

    @Override
    public HttpResponse httpDelete(final String path) throws IOException {
        return this.httpDelete(path, null);
    }

    @Override
    public HttpResponse httpDelete(final String path,
                                   final MantaHttpHeaders headers) throws IOException {
        Validate.notNull(path, "Path must not be null");

        LOGGER.debug("DELETE {}", path);

        final HttpDelete delete = requestFactory.delete(path);
        if (headers != null) {
            MantaHttpRequestFactory.addHeaders(delete, headers.asApacheHttpHeaders());
        }

        final CloseableHttpResponse response = executeAndCloseRequest(delete, "DELETE {} response [{}] {} ");
        final int code = response.getStatusLine().getStatusCode();

        // any of the following are valid response codes for DELETE
        // though manta currently only returns SC_NO_CONTENT (204)
        // general error response codes (>=400) like SC_PRECONDITION_FAILED are validated by executeAndCloseRequest
        if (code != SC_OK
                && code != SC_ACCEPTED
                && code != SC_NO_CONTENT) {
            throw new MantaClientHttpResponseException(delete,
                                                       response,
                                                       path,
                                                       SC_OK, SC_ACCEPTED, SC_NO_CONTENT);
        }

        return response;
    }


    @Override
    public HttpResponse httpPost(final String path) throws IOException {
        return httpPost(path, null, null);
    }

    @Override
    public HttpResponse httpPost(final String path,
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

        final HttpPost post = requestFactory.post(path);
        MantaHttpRequestFactory.addHeaders(post, httpHeaders.asApacheHttpHeaders());

        if (entity != null) {
            post.setEntity(entity);
        }

        return executeAndCloseRequest(post, (Integer) null,
                "POST   {} response [{}] {} ");
    }

    @Override
    public MantaObjectResponse httpPut(final String path,
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

        final HttpPut put = requestFactory.put(path);
        MantaHttpRequestFactory.addHeaders(put, httpHeaders.asApacheHttpHeaders());

        final DigestedEntity md5DigestedEntity;

        if (entity != null) {
            if (this.verifyUploads) {
                md5DigestedEntity = new DigestedEntity(entity, new FastMD5Digest());
                put.setEntity(md5DigestedEntity);
            } else {
                md5DigestedEntity = null;
                put.setEntity(entity);
            }
        } else {
            md5DigestedEntity = null;
        }

        final CloseableHttpClient client = connectionContext.getHttpClient();
        final MantaObjectResponse obj;

        try (CloseableHttpResponse response = client.execute(put)) {
            StatusLine statusLine = response.getStatusLine();
            LOGGER.debug("PUT    {} response [{}] {} ", path, statusLine.getStatusCode(),
                    statusLine.getReasonPhrase());
            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(response.getAllHeaders());
            // We add back in the metadata made in the request so that it is easily available
            responseHeaders.putAll(httpHeaders.metadata());

            obj = new MantaObjectResponse(path, responseHeaders, metadata);

            if (statusLine.getStatusCode() != HttpStatus.SC_NO_CONTENT) {
                throw new MantaClientHttpResponseException(put, response,
                        put.getURI().getPath());
            }

            if (this.verifyUploads) {
                validateChecksum(md5DigestedEntity, obj.getMd5Bytes(), put, response);
            }
        }

        /* We set the content type on the result object from the entity
         * PUT if that content type isn't already present on the result object.
         * This allows for the result object to have the original
         * content-type even if it isn't part of any response headers. */
        if (obj.getContentType() == null && entity != null && entity.getContentType() != null) {
            obj.setContentType(entity.getContentType().getValue());
        }


        return obj;
    }

    @Override
    public MantaObjectResponse httpPutMetadata(final String path,
                                               final MantaHttpHeaders headers,
                                               final MantaMetadata metadata) throws IOException {
        headers.putAll(metadata);
        headers.setContentEncoding("chunked");

        List<NameValuePair> pairs = Collections.singletonList(new BasicNameValuePair("metadata", "true"));

        HttpPut put = requestFactory.put(path, pairs);
        MantaHttpRequestFactory.addHeaders(put, headers.asApacheHttpHeaders());
        put.setEntity(NoContentEntity.INSTANCE);

        try (CloseableHttpResponse response = executeRequest(
                put, HttpStatus.SC_NO_CONTENT,
                "PUT    {} response [{}] {} ")) {

            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(
                    response.getAllHeaders());

            MantaObjectResponse obj = new MantaObjectResponse(path,
                    responseHeaders, metadata);

            HttpEntity entity = response.getEntity();

            if (obj.getContentType() == null && entity != null
                    && entity.getContentType() != null) {
                obj.setContentType(entity.getContentType().getValue());
            }

            return obj;
        }
    }

    @Override
    public MantaObjectInputStream httpRequestAsInputStream(final HttpUriRequest request,
                                                           final MantaHttpHeaders requestHeaders)
            throws IOException {
        if (requestHeaders != null) {
            MantaHttpRequestFactory.addHeaders(request, requestHeaders.asApacheHttpHeaders());
        }

        final int expectedHttpStatus;

        if (requestHeaders != null && requestHeaders.containsKey(HttpHeaders.RANGE)) {
            expectedHttpStatus = HttpStatus.SC_PARTIAL_CONTENT;
        } else {
            expectedHttpStatus = HttpStatus.SC_OK;
        }

        final Function<CloseableHttpResponse, MantaObjectInputStream> responseAction = response -> {
            final MantaHttpHeaders responseHeaders = new MantaHttpHeaders(response.getAllHeaders());
            final String path = request.getURI().getPath();
            // MantaObjectResponse expects to be constructed with the
            // encoded path, which it then decodes when a caller does
            // getPath.  However, here the HttpUriRequest has already
            // decoded.
            final MantaObjectResponse metadata = new MantaObjectResponse(MantaUtils.formatPath(path), responseHeaders);

            if (metadata.isDirectory()) {
                final String msg = "Directories do not have data, so data streams "
                        + "from directories are not possible.";
                final MantaUnexpectedObjectTypeException exception =
                        new MantaUnexpectedObjectTypeException(msg,
                            ObjectType.FILE, ObjectType.DIRECTORY);
                exception.setContextValue("path", path);

                if (metadata.getHttpHeaders() != null) {
                    exception.setResponseHeaders(metadata.getHttpHeaders());
                }

                throw exception;
            }

            final HttpEntity entity = response.getEntity();
            if (entity == null) {
                final String msg = "Can't process null response entity.";
                final MantaClientException exception = new MantaClientException(msg);
                exception.setContextValue("uri", request.getRequestLine().getUri());
                exception.setContextValue("method", request.getRequestLine().getMethod());

                throw exception;
            }

            final InputStream httpEntityStream;
            try {
                httpEntityStream = entity.getContent();
            } catch (IOException ioe) {
                String msg = String.format("Error getting stream from entity content for path: %s",
                        path);
                MantaObjectException e = new MantaObjectException(msg, ioe);
                e.setContextValue("path", path);
                HttpHelper.annotateContextedException(e, request, response);
                throw e;
            }

            final InputStream backingStream;
            final InputStreamContinuator continuator = constructContinuatorForCompatibleRequest(request, response);
            if (continuator != null) {
                backingStream = new AutoContinuingInputStream(httpEntityStream, continuator);
            } else {
                backingStream = httpEntityStream;
            }

            return new MantaObjectInputStream(metadata, response, backingStream);
        };

        return executeRequest(
                request,
                expectedHttpStatus,
                responseAction,
                false,
                "GET    {} response [{}] {} ");
    }

    /**
     * Attempts to construct a {@link InputStreamContinuator} for the initial request-response exchange. If the request
     * cannot be resumed (either because the exchange is malformed or the request does not support resuming) we will
     * return null, otherwise the continuator receives our client, the initial request (which it will clone) and the
     * marker we created.
     *
     * @see HttpDownloadContinuationMarker#validateInitialExchange
     *
     * @param request the initial request, etag and range headers will be used as hints for the first argument to
     * {@link HttpDownloadContinuationMarker#validateInitialExchange(Pair, int, Pair)}
     * @param response the initial response which will be validated against any hints
     * @return the continuator which can be used to resume this request
     */
    private InputStreamContinuator constructContinuatorForCompatibleRequest(final HttpUriRequest request,
                                                                            final CloseableHttpResponse response) {
        if (this.maxDownloadContinuations == DOWNLOAD_CONTINUATIONS_DISABLED
                || !HttpGet.METHOD_NAME.equalsIgnoreCase(request.getMethod())
                || !(this.connectionContext instanceof MantaApacheHttpClientContext)
                || !(request instanceof HttpGet)) {
            return null;
        }

        final HttpGet get = (HttpGet) request;
        final HttpDownloadContinuationMarker marker;
        try {
            // if we can't build a marker the request:
            // - uses a combination of headers we don't support (e.g. multi-part range)
            // - the request/response pair exhibits unexpected behavior we are not prepared to follow
            marker = validateInitialExchange(extractDownloadRequestFingerprint(get),
                                             response.getStatusLine().getStatusCode(),
                                             extractDownloadResponseFingerprint(response, true));
        } catch (final ProtocolException pe) {
            LOGGER.debug("HTTP download cannot be automatically continued: {}", pe.getMessage());
            return null;
        }

        try {
            return new ApacheHttpGetResponseEntityContentContinuator(
                    (MantaApacheHttpClientContext) this.connectionContext,
                    get,
                    marker,
                    this.maxDownloadContinuations);
        } catch (final HttpDownloadContinuationException rde) {
            LOGGER.debug(
                    String.format(
                            "Expected to build a continuator but an exception occurred: %s",
                            rde.getMessage()));
        }

        return null;
    }

    /**
     * Checks to make sure that the uploaded entity's MD5 matches the MD5 as calculated on the server. This check is
     * skipped if the entity is null.
     *
     * @param entity null or the entity object
     * @param serverMd5 service side computed MD5 value
     * @param request HTTP request object
     * @param response HTTP response object
     * @throws MantaChecksumFailedException thrown if the MD5 values do not match
     */
    protected static void validateChecksum(final DigestedEntity entity,
                                           final byte[] serverMd5,
                                           final HttpRequest request,
                                           final HttpResponse response)
            throws MantaChecksumFailedException {
        Validate.notNull(entity, "Request body required");

        if (serverMd5 == null || serverMd5.length == 0) {
            final String msg = "Server calculated MD5 is missing";
            throw new MantaChecksumFailedException(msg, request, response);
        }

        final byte[] clientMd5 = entity.getDigest();
        final boolean areMd5sTheSame = Arrays.equals(serverMd5, clientMd5);

        if (!areMd5sTheSame) {
            String msg = "Client calculated MD5 and server calculated MD5 do not match";
            MantaChecksumFailedException e = new MantaChecksumFailedException(msg, request, response);
            e.setContextValue("serverMd5", MantaUtils.byteArrayAsHexString(serverMd5));
            e.setContextValue("clientMd5", MantaUtils.byteArrayAsHexString(clientMd5));

            throw e;
        }
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        return executeRequest(request, null, logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final Integer expectedStatusCode,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request);
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

    @Override
    public CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                        final String logMessage,
                                                        final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, (Integer) null, logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeAndCloseRequest(final HttpUriRequest request,
                                                        final Integer expectedStatusCode,
                                                        final String logMessage,
                                                        final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, true,
                logMessage, logParameters);
    }

    @Override
    public CloseableHttpResponse executeRequest(final HttpUriRequest request,
                                                final Integer expectedStatusCode,
                                                final boolean closeResponse,
                                                final String logMessage,
                                                final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();
        CloseableHttpResponse response = client.execute(request);

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

    @Override
    public <R> R executeAndCloseRequest(final HttpUriRequest request,
                                        final Function<CloseableHttpResponse, R> responseAction,
                                        final String logMessage,
                                        final Object... logParameters)
            throws IOException {
        return executeAndCloseRequest(request, null,
                responseAction, logMessage, logParameters);
    }

    @Override
    public <R> R executeAndCloseRequest(final HttpUriRequest request,
                                        final Integer expectedStatusCode,
                                        final Function<CloseableHttpResponse, R> responseAction,
                                        final String logMessage,
                                        final Object... logParameters)
            throws IOException {
        return executeRequest(request, expectedStatusCode, responseAction,
                true, logMessage, logParameters);
    }

    @Override
    public <R> R executeRequest(final HttpUriRequest request,
                                final Integer expectedStatusCode,
                                final Function<CloseableHttpResponse, R> responseAction,
                                final boolean closeResponse,
                                final String logMessage,
                                final Object... logParameters)
            throws IOException {
        Validate.notNull(request, "Request object must not be null");

        CloseableHttpClient client = connectionContext.getHttpClient();

        CloseableHttpResponse response = client.execute(request);
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
     * Utility method that determines if a request failed by comparing the status code to an expectation if present
     * (non-null) or by finding out if the HTTP status code is less than 400.
     *
     * @param expectedStatusCode null for default behavior or the specific status code
     * @param statusLine status line object containing status code
     * @return boolean true indicates the request failed
     */
    protected static boolean isFailedStatusCode(final Integer expectedStatusCode,
                                                final StatusLine statusLine) {
        int code = statusLine.getStatusCode();

        if (expectedStatusCode == null) {
            return code >= HttpStatus.SC_BAD_REQUEST;
        } else {
            return code != expectedStatusCode;
        }
    }

    /**
     * Check if we can safely provide download continuation.
     *
     * @param connCtx the connection context which should also be a {@link RetryConfigAware}
     * @param downloadContinuations the desired number of continuations
     * @return if it is safe to enable download continuation
     */
    private static Integer validateDownloadContinuationConditions(final MantaConnectionContext connCtx,
                                                                  final Integer downloadContinuations) {
        if (!(connCtx instanceof MantaApacheHttpClientContext)) {
            return DOWNLOAD_CONTINUATIONS_DISABLED;
        }

        final boolean continuationDisabled = downloadContinuations == null || downloadContinuations == 0;

        if (continuationDisabled) {
            return DOWNLOAD_CONTINUATIONS_DISABLED;
        }

        final MantaApacheHttpClientContext apacheConnCtx = (MantaApacheHttpClientContext) connCtx;
        // the final condition is that the client either has retries disabled or supports cancellation
        if (apacheConnCtx.isRetryCancellable()) {
            return downloadContinuations;
        }

        if (!apacheConnCtx.isRetryEnabled()) {
            return downloadContinuations;
        }

        // otherwise disable
        return DOWNLOAD_CONTINUATIONS_DISABLED;
    }

    @Override
    public void close() throws Exception {
        connectionContext.close();
    }
}
