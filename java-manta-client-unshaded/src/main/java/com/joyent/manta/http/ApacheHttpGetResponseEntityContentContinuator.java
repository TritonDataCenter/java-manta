/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.config.MantaClientMetricConfiguration;
import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.AbstractExecutionAwareRequest;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.net.ssl.SSLException;

import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadResponseFingerprint;
import static com.joyent.manta.http.HttpContextRetryCancellation.CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;

/**
 * Manages state needed to repeatedly "resume" a download (as an {@link java.io.InputStream} by updating the {@code
 * Range} header whenever non-fatal resonse errors occur. Uses {@code If-Match} to make sure the object being downloaded
 * has not been changed between download segments. Additionally validates that the returned {@code ETag} does actually
 * match the {@code If-Match} header and that the returned {@code Content-Range} does actually match the requested
 * {@code Range} header.
 * <p>
 * Note: closing a continuator frees any resources that continuator owns and records the number of continuations
 * provided by that continuator, it should <strong>NOT</strong> close the provided {@link HttpClient}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>h
 * @since 3.2.3
 */
public class ApacheHttpGetResponseEntityContentContinuator implements InputStreamContinuator {

    /**
     * Set of exceptions from which we know we cannot recover by simply retrying. Since
     * {@link java.net.SocketTimeoutException} is an {@link java.io.InterruptedIOException} we omit
     * that class from this list.
     *
     * @see org.apache.http.impl.client.DefaultHttpRequestRetryHandler#nonRetriableClasses
     * @see MantaHttpRequestRetryHandler#NON_RETRIABLE
     */
    private static final Set<Class<? extends IOException>> EXCEPTIONS_FATAL =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    UnknownHostException.class,
                                    ConnectException.class,
                                    SSLException.class)));

    /**
     * Prefix used to build metric names for exceptions from which this continuator has helped recover.
     */
    static final String METRIC_NAME_PREFIX_METER_RECOVERED = "get-continuations-recovered-";

    /**
     * The key under which the distribution of continuations delivered per request is recorded.
     */
    static final String METRIC_NAME_CONTINUATIONS_PER_REQUEST = "get-continuations-per-request-distribution";

    /**
     * The HTTP client.
     */
    private final HttpClient client;

    /**
     * A clone of the original user-supplied request. We need to defensively clone the request in case the user intends
     * to reuse it themselves since we modify headers.
     */
    private final HttpGet request;

    /**
     * Number of continuations we have supplied.
     */
    private int continuation;

    /**
     * Information recorded about the initial request/response exchange we can used to validateResponseWithMarker
     * response headers for continuations.
     */
    private final ResumableDownloadMarker marker;

    /**
     * Nullable metric registry for tracking exceptions seen.
     */
    private final MetricRegistry metricRegistry;

    /**
     * Histogram of the number of continuations built by a single instance, in addition to total continuations (since
     * histograms also keep the number of samples recorded).
     */
    private final Histogram continuationsDeliveredDistribution;

    /**
     * Construct a coordinator. Each download request requires a new continuator. Invariants required by this class will
     * be checked including whether the provided headers and the supplied client context's retry configuration are
     * compatible with this implementation.
     *
     * @param connCtx the http connection context
     * @param request the initial request
     * @param marker the relevant information from the initial exchange
     * @throws ResumableDownloadIncompatibleRequestException when the initial request is incompatible with this
     * implementation
     * @throws ResumableDownloadUnexpectedResponseException when the initial response diverges from the request headers
     * @throws ResumableDownloadException when something unexpected happens while preparing
     */
    ApacheHttpGetResponseEntityContentContinuator(final MantaApacheHttpClientContext connCtx,
                                                  final HttpGet request,
                                                  final ResumableDownloadMarker marker)
            throws ResumableDownloadException {
        this(verifyDownloadContinuationIsSafeAndExtractHttpClient(connCtx),
             request,
             marker,
             extractMetricRegistry(connCtx));
    }

    /**
     * Package-private constructor for unit-testing methods which do execute continuation requests (and can prepare
     * their own client which obeys our retry rules).
     *
     * @param client the client we'll use to make requests for the remaining data
     * @param request the initial request
     * @param marker the relevant information from the initial exchange
     * @param metricRegistry registry for building the total continuations {@link Counter}
     */
    ApacheHttpGetResponseEntityContentContinuator(final HttpClient client,
                                                  final HttpGet request,
                                                  final ResumableDownloadMarker marker,
                                                  final MetricRegistry metricRegistry) {
        // we clone the request in case the user is reusing the same request object
        this.request = cloneRequest(request);
        this.marker = requireNonNull(marker);
        this.request.setHeader(IF_MATCH, this.marker.getEtag());
        this.request.setHeader(RANGE, this.marker.getCurrentRange().render());

        this.client = requireNonNull(client);
        this.continuation = 0;

        if (metricRegistry != null) {
            this.metricRegistry = metricRegistry;
            this.continuationsDeliveredDistribution = metricRegistry.histogram(METRIC_NAME_CONTINUATIONS_PER_REQUEST);
        } else {
            this.metricRegistry = null;
            this.continuationsDeliveredDistribution = null;
        }
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
    public InputStream buildContinuation(final IOException ex, final long bytesRead) throws IOException {
        if (!isRecoverable(ex)) {
            throw ex;
        }

        this.continuation++;
        if (this.metricRegistry != null) {
            this.metricRegistry.counter(METRIC_NAME_PREFIX_METER_RECOVERED + ex.getClass().getSimpleName()).inc();
        }

        try {
            this.marker.updateRangeStart(bytesRead);
        } catch (final IllegalArgumentException iae) {
            // we should wrap and rethrow this so that the caller doesn't get stuck in a loop
            throw new ResumableDownloadException("Failed to update download continuation offset", iae);
        }

        this.request.setHeader(RANGE, this.marker.getCurrentRange().render());

        // not yet trying to handle exceptions during request execution
        final HttpResponse response;
        try {
            final HttpContext httpContext = new BasicHttpContext();
            httpContext.setAttribute(CONTEXT_ATTRIBUTE_MANTA_RETRY_DISABLE, true);
            response = this.client.execute(this.request, httpContext);
        } catch (final IOException ioe) {
            throw new ResumableDownloadException(
                    "Exception occurred while attempting to build continuation: " + ioe.getMessage(),
                    ioe);
        }

        final int statusCode = response.getStatusLine().getStatusCode();

        if (statusCode != SC_PARTIAL_CONTENT) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Invalid response code: expecting [%d], got [%d]",
                            SC_PARTIAL_CONTENT,
                            statusCode));
        }

        try {
            validateResponseWithMarker(extractDownloadResponseFingerprint(response, false));
        } catch (final HttpException he) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Continuation request failed validation: " + he.getMessage(),
                    he);
        }

        final InputStream content;
        try {
            final HttpEntity entity = response.getEntity();

            if (entity == null) {
                throw new ResumableDownloadUnexpectedResponseException(
                        "Entity missing from continuation response");
            }

            content = entity.getContent();

            if (content == null) {
                throw new ResumableDownloadUnexpectedResponseException(
                        "Entity content missing from continuation response");
            }
        } catch (final UnsupportedOperationException | IOException uoe) {
            throw new ResumableDownloadUnexpectedResponseException(uoe);
        }

        return content;
    }

    /**
     * Determine whether an {@link IOException} indicates a fatal issue or not. Shamelessly plagiarized from {@link
     * org.apache.http.impl.client.DefaultHttpRequestRetryHandler}.
     *
     * @param e the exception to check
     * @return whether or not the caller should retry their request
     */
    private static boolean isRecoverable(final IOException e) {
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

    /**
     * Package-private method which compares a new response fingerprint with the internal marker
     *
     * @param responseFingerprint the continuation response etag+contentrange
     * @throws ResumableDownloadUnexpectedResponseException if the response does not match the marker's expectations
     */
    void validateResponseWithMarker(final Pair<String, HttpRange.Response> responseFingerprint)
            throws ResumableDownloadUnexpectedResponseException {
        notNull(responseFingerprint, "Response fingerprint must not be null");

        if (!this.marker.getEtag().equals(responseFingerprint.getLeft())) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Response ETag mismatch: expected [%s], got [%s]",
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
                    "Response Content-Range mismatch: " + e.getMessage(),
                    e);
        }
    }

    /**
     * Method indicating that this continuator is no longer required. Currently only used to record the number of
     * continuations that where used for this particular request.
     */
    @Override
    public void close() {
        if (this.continuationsDeliveredDistribution == null) {
            return;
        }

        this.continuationsDeliveredDistribution.update(this.continuation);
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

    @SuppressWarnings("checkstyle:JavadocMethod")
    private static HttpClient verifyDownloadContinuationIsSafeAndExtractHttpClient(
            final MantaApacheHttpClientContext connCtx) throws ResumableDownloadException {
        notNull(connCtx, "Connection context must not be null");

        final boolean cancellable = connCtx.isRetryCancellable();
        final boolean enabled = connCtx.isRetryEnabled();
        if (enabled && !cancellable) {
            throw new ResumableDownloadException("Incompatible connection context, automatic retries must be "
                                                         + "disabled or cancellable");
        }

        return requireNonNull(connCtx.getHttpClient());
    }

    private static MetricRegistry extractMetricRegistry(final MantaApacheHttpClientContext connCtx) {
        final MantaClientMetricConfiguration metricConfig = connCtx.getMetricConfig();
        if (metricConfig == null) {
            return null;
        }

        return metricConfig.getRegistry();
    }
}
