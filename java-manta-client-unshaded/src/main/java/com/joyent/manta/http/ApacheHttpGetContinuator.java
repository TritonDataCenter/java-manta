/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.AbstractExecutionAwareRequest;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.joyent.manta.http.HttpContextRetryCancellation.CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;

/**
 * Manages state needed to repeatedly "resume" a download (as an {@link java.io.InputStream} by updating the {@code
 * Range} header whenever non-fatal resonse errors occur. Uses {@code If-Match} to make sure the object being downloaded
 * has not been changed between download segments. Additionally validates that the returned {@code ETag} does actually
 * match the {@code If-Match} header and that the returned {@code Content-Range} does actually match the requested
 * {@code Range} header.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>h
 * @since 3.2.3
 */
public class ApacheHttpGetContinuator implements HttpGetContinuator {

    /**
     * Set of exceptions from which we know we cannot recover by simply retrying.
     */
    private static final Set<Class<? extends IOException>> EXCEPTIONS_FATAL =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    InterruptedIOException.class,
                                    UnknownHostException.class,
                                    ConnectException.class,
                                    SSLException.class)));

    /**
     * Constant representing that continuations should be supplied indefinitely (as long as other invariants hold).
     */
    static final int INFINITE_CONTINUATIONS = -1;

    /**
     * The key under which the rate of continuation requests is recorded in the metric registry.
     */
    private static final String METRIC_NAME_GET_CONTINUATIONS = "get-continuations";

    /**
     * The HTTP client.
     */
    private final CloseableHttpClient client;

    /**
     * A clone of the original user-supplied request. We need to defensively clone the request in case the user intends
     * to reuse it themselves since we modify headers.
     */
    private final HttpGet request;

    /**
     * Maximum number of continuations this resumer will supply.
     */
    private final int retryCount;

    /**
     * Number of continuations this resumer has supplied.
     */
    private int continuation;

    /**
     * Information recorded about the initial request/response exchange we can used to validateResponseWithMarker
     * response headers for continuations.
     */
    private final ResumableDownloadMarker marker;

    /**
     * Metric for tracking all continuations seen by a single registry.
     */
    private final Counter totalContinuations;

    /**
     * Construct a coordinator. Each logical download should use a different coordinator in order to simplify state
     * management.
     *
     * @param connCtx the http connection context
     * @param request the initial request
     * @param initialResponse the first response received
     * @param retryCount the number of continuations to attempt, or -1 for infinite continuations
     * @throws ResumableDownloadIncompatibleRequestException when the initial request is incompatible with this
     * implementation
     * @throws ResumableDownloadUnexpectedResponseException when the initial response diverges from the request headers
     * @throws ResumableDownloadException when something unexpected happens while preparing the resumer
     */
    public ApacheHttpGetContinuator(final MantaApacheHttpClientContext connCtx,
                                    final HttpGet request,
                                    final HttpResponse initialResponse,
                                    final int retryCount)
            throws ResumableDownloadException {
        this(connCtx, request, initialResponse, retryCount, null);
    }

    /**
     * Construct a coordinator. Each logical download should use a different coordinator in order to simplify state
     * management.
     *
     * @param connCtx the http connection context
     * @param request the initial request
     * @param initialResponse the first response received
     * @param retryCount the number of continuations to attempt, or -1 for infinite continuations
     * @param metricRegistry an optional registry for tracking the rate at which continuations are being triggered
     * @throws ResumableDownloadIncompatibleRequestException when the initial request is incompatible with this
     * implementation
     * @throws ResumableDownloadUnexpectedResponseException when the initial response diverges from the request headers
     * @throws ResumableDownloadException when something unexpected happens while preparing the resumer
     */
    public ApacheHttpGetContinuator(final MantaApacheHttpClientContext connCtx,
                                    final HttpGet request,
                                    final HttpResponse initialResponse,
                                    final int retryCount,
                                    final MetricRegistry metricRegistry)
            throws ResumableDownloadException {
        this(verifyDownloadContinuationIsSafeAndExtractHttpClient(connCtx),
                request,
                initialResponse,
                retryCount,
                metricRegistry);
    }

    /**
     * Package-private constructor for unit-testing methods which do not execute continuation requests.
     *
     * @param request the initial request
     * @param initialResponse the first response received
     * @throws ResumableDownloadIncompatibleRequestException when the initial request is incompatible with this
     * implementation
     * @throws ResumableDownloadUnexpectedResponseException when the initial response diverges from the request headers
     * @throws ResumableDownloadException when something unexpected happens while preparing the resumer
     */
    ApacheHttpGetContinuator(final HttpGet request, final HttpResponse initialResponse)
            throws ResumableDownloadException {
        this((CloseableHttpClient) null, request, initialResponse, INFINITE_CONTINUATIONS, null);
    }

    /**
     * Package-private constructor for unit-testing methods which do execute continuation requests (and can prepare
     * their own client which obeys our retry rules).
     *
     * @param request the initial request
     * @param initialResponse the first response received
     * @param retryCount the number of continuations to attempt, or -1 for infinite continuations
     * @throws ResumableDownloadIncompatibleRequestException when the initial request is incompatible with this
     * implementation
     * @throws ResumableDownloadUnexpectedResponseException when the initial response diverges from the request headers
     * @throws ResumableDownloadException when something unexpected happens while preparing the resumer
     */
    ApacheHttpGetContinuator(final CloseableHttpClient client,
                             final HttpGet request,
                             final HttpResponse initialResponse,
                             final int retryCount,
                             final MetricRegistry metricRegistry)
            throws ResumableDownloadException {
        // we clone and verify the request before assigning it to our field
        final HttpGet cloned = cloneRequest(request);

        // one or both (or neither) hints may be present
        final Pair<String, HttpRange.Request> hints = ensureRequestHeadersAreCompatible(cloned);
        final Pair<String, HttpRange.Response> initialResponseFingerprint = extractResponseFingerprint(initialResponse);

        this.marker = ResumableDownloadMarker.validateInitialExchange(hints, initialResponseFingerprint);
        cloned.setHeader(IF_MATCH, this.marker.getEtag());
        cloned.setHeader(RANGE, this.marker.getCurrentRange().render());

        this.request = cloned;
        this.client = client;
        this.retryCount = validateRetryCount(retryCount);
        this.continuation = 0;
        this.totalContinuations = extractContinuationCounterOrNull(metricRegistry);
    }

    /**
     * Get an {@link InputStream} which picks up starting {@code bytesRead} bytes from the beginning of the logical
     * object being downloaded. Implementations should compare headers across all requests and responses to ensure that
     * the object being downloaded has not changed between the initial and subsequent requests.
     *
     * @param ex the exception which occurred while downloading (either the first response or a continuation)
     * @param bytesRead byte offset at which the new stream should start
     * @return another stream which continues to deliver the bytes from the initial request
     * @throws ResumableDownloadException if the provided {@link IOException} is not recoverable or the number of
     * retries has been reached, or there is an error
     * @throws ResumableDownloadUnexpectedResponseException if the continuation response was incompatible or indicated
     * that the remote object has somehow changed
     */
    @Override
    public InputStream buildContinuation(final IOException ex, final long bytesRead) throws ResumableDownloadException {
        if (!this.isRecoverable(ex)) {
            throw new ResumableDownloadException("IOException is not recoverable", ex);
        }

        this.continuation++;
        if (totalContinuations != null) {
            totalContinuations.inc();
        }

        if (this.retryCount != INFINITE_CONTINUATIONS && this.continuation <= this.retryCount) {
            throw new ResumableDownloadException("Maximum continuation count reached.");
        }

        this.marker.updateRangeStart(bytesRead);
        this.request.setHeader(RANGE, this.marker.getCurrentRange().render());

        // not yet trying to handle exceptions during request execution
        final CloseableHttpResponse response;
        try {
            final HttpContext httpContext = new BasicHttpContext();
            httpContext.setAttribute(CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE, true);
            response = this.client.execute(this.request, httpContext);
        } catch (final IOException ioe) {
            throw new ResumableDownloadException(ioe);
        }

        final int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode != SC_PARTIAL_CONTENT) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Invalid response code: expecting [%d], got [%d]",
                            SC_PARTIAL_CONTENT,
                            statusCode));
        }

        validateResponseWithMarker(extractResponseFingerprint(response));

        final InputStream content;
        try {
            final HttpEntity entity = response.getEntity();

            if (entity == null) {
                throw new ResumableDownloadUnexpectedResponseException("Entity missing from resumed response");
            }

            content = entity.getContent();

            if (content == null) {
                throw new ResumableDownloadUnexpectedResponseException("Entity content missing from resumed response");
            }
        } catch (final UnsupportedOperationException | IOException uoe) {
            throw new ResumableDownloadUnexpectedResponseException(uoe);
        }

        return content;
    }

    /**
     * Determine whether an {@link IOException} indicates a fatal issue or not. Shamelessly plaigarized from {@link
     * org.apache.http.impl.client.DefaultHttpRequestRetryHandler}.
     *
     * @param e the exception to check
     * @return whether or not the caller should retry their request
     */
    private boolean isRecoverable(final IOException e) {
        if (EXCEPTIONS_FATAL.contains(e.getClass())) {
            return false;
        }

        for (final Class<? extends IOException> exceptionClass : EXCEPTIONS_FATAL) {
            if (exceptionClass.isInstance(e)) {
                return false;
            }
        }

        return true;
    }

    void validateResponseWithMarker(final Pair<String, HttpRange.Response> responseFingerprint)
            throws ResumableDownloadUnexpectedResponseException {
        notNull(responseFingerprint, "Response fingerprint must not be null");

        if (!this.marker.getEtag().equals(responseFingerprint.getLeft())) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Response missing ETag does not match marker: expected [%s], got [%s]",
                            this.marker.getEtag(),
                            responseFingerprint.getLeft()));
        }

        if (responseFingerprint.getRight() == null) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Response missing Content-Range and Content-Length");
        }

        // ask the marker to make sure we got the expected range back
        try {
            this.marker.validateResponseRange(responseFingerprint.getRight());
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Invalid Content-Range for resumed download response", e);
        }
    }

    /**
     * Clone a request so we can freely modify headers when retrieving a continuation {@link InputStream} in {@link
     * #buildContinuation(IOException, long)}. This method is necessary because {@link
     * AbstractExecutionAwareRequest#clone()} is basically useless.
     *
     * @param request the request being cloned
     * @return the cloned request
     */
    static HttpGet cloneRequest(final HttpGet request) {
        final HttpGet get = new HttpGet(request.getURI());

        // deep-clone headers
        for (final Header hdr : request.getAllHeaders()) {
            get.addHeader(hdr.getName(), hdr.getValue());
        }

        return get;
    }

    /**
     * Checks that a request has headers which are compatible. This means that the request either: 1. has no If-Match
     * header and no ETag header 2. has a single-valued If-Match header 3. has a single Range header specifying a single
     * byte range (i.e. no multipart ranges) 4. satisfies both #2 and #3
     *
     * @param request the request being checked for compatibility
     * @return the ETag and range hints to be validated against the initial response
     * @throws ResumableDownloadIncompatibleRequestException when the request cannot be resumed
     */
    static Pair<String, HttpRange.Request> ensureRequestHeadersAreCompatible(final HttpRequest request)
            throws ResumableDownloadIncompatibleRequestException {
        String ifMatch = null;
        HttpRange.Request range = null;

        Exception ifMatchEx = null;
        Exception rangeEx = null;

        try {
            ifMatch = ApacheHttpHeaderUtils.extractSingleHeaderValue(request, IF_MATCH, false);
        } catch (final HttpException e) {
            ifMatchEx = e;
        }

        try {
            final String rawRequestRange = ApacheHttpHeaderUtils.extractSingleHeaderValue(request, RANGE, false);
            if (rawRequestRange != null) {
                range = HttpRange.parseRequestRange(rawRequestRange);
            }
        } catch (final HttpException e) {
            rangeEx = e;
        }

        if (ifMatchEx != null && rangeEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Incompatible Range and If-Match request headers for resuming download:%n%s%n%s",
                            rangeEx.getMessage(),
                            ifMatchEx.getMessage()));
        } else if (ifMatchEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(ifMatchEx);
        } else if (rangeEx != null) {
            throw new ResumableDownloadIncompatibleRequestException(rangeEx);
        }

        return ImmutablePair.of(ifMatch, range);
    }

    /**
     * In order to be sure we're continuing to download the same object we need to extract the {@code ETag} and {@code
     * Content-Range} headers from the response. Either header missing is an error. Additionally, when the {@code
     * Content-Range} header is present the specified range should be equal to the response's {@code Content-Length}.
     *
     * @param response the response to check for headers
     * @return the request headers we're concerned with validating
     * @throws ResumableDownloadUnexpectedResponseException when the headers are malformed, unparseable, or the {@code
     * Content-Range} and {@code Content-Length} are mismatched
     */
    private static Pair<String, HttpRange.Response> extractResponseFingerprint(final HttpResponse response)
            throws ResumableDownloadUnexpectedResponseException {

        final String etag;
        try {
            etag = ApacheHttpHeaderUtils.extractSingleHeaderValue(response, ETAG, true);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        final long contentLength;
        try {
            final String rawContentLength = ApacheHttpHeaderUtils.extractSingleHeaderValue(response, CONTENT_LENGTH,
                    true);
            contentLength = Long.parseUnsignedLong(notNull(rawContentLength)); // notNull squelches
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        } catch (final NumberFormatException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Failed to parse Content-Length response, matching headers: %s",
                            Arrays.deepToString(response.getHeaders(CONTENT_LENGTH))));
        }

        final String rawContentRange;
        try {
            rawContentRange = ApacheHttpHeaderUtils.extractSingleHeaderValue(response, CONTENT_RANGE, false);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        if (StringUtils.isBlank(rawContentRange)) {
            // the entire object is being requested
            return new ImmutablePair<>(etag, new HttpRange.Response(0, contentLength - 1, contentLength));
        }

        final HttpRange.Response contentRange;
        try {
            contentRange = HttpRange.parseContentRange(rawContentRange);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Unable to parse Content-Range header while analyzing download response",
                    e);
        }

        // Manta follows the spec and sends the Content-Length of the range, which we should validateResponseWithMarker
        if (contentRange.contentLength() != contentLength) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Invalid Content-Length in range response: expected [%d], got [%d]",
                            contentRange.contentLength(),
                            contentLength));
        }

        return new ImmutablePair<>(etag, contentRange);
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private static CloseableHttpClient verifyDownloadContinuationIsSafeAndExtractHttpClient(
            final MantaApacheHttpClientContext connCtx) throws ResumableDownloadException {
        notNull(connCtx, "Connection context must not be null");


        final boolean cancellable = connCtx.isRetryCancellable();
        final boolean enabled = connCtx.isRetryEnabled();
        if (enabled && !cancellable) {
            throw new ResumableDownloadException("Incompatible connection context, automatic retries must be "
                    + "disabled or cancellable");
        }

        final CloseableHttpClient client = connCtx.getHttpClient();
        return notNull(client, "Connection context missing HttpClient");
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private static int validateRetryCount(final int retryCount) {
        if (retryCount <= 0 && retryCount != INFINITE_CONTINUATIONS) {
            throw new IllegalArgumentException("Retry count must be either -1 (infinite) or greater than zero");
        }

        return retryCount;
    }

    @SuppressWarnings("checkstyle:JavadocMethod")
    private static Counter extractContinuationCounterOrNull(final MetricRegistry metricRegistry) {
        if (metricRegistry == null) {
            return null;
        }

        return metricRegistry.counter(METRIC_NAME_GET_CONTINUATIONS);
    }
}
