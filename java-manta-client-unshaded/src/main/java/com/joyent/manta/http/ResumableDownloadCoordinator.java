/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;

// @formatter:off
/**
 *
 * Manages state needed to "resume" a download by updating the {@code Range} header in subsequent retries.
 * States:
 *  - ready: freshly constructed, registers self into HttpContext
 *      - NEXT: start
 *
 *  - start: the initial response has arrived, {@link ResumableDownloadHttpResponseInterceptor} gives us a
 *      {@link ResumableDownloadMarker} before handing the response off to the caller so that we can:
 *      1. ensure the ETag remains unchanged across responses
 *      2. ensure the total object size across responses
 *      3. keep track of the current request range
 *      - NEXT: recover
 *
 *  - recover: at any point while the download is in flight (basically any state but "cancelled") an IOException may be
 *      caught and passed to {@link #attemptRecovery(IOException)}, which can do one of the following:
 *      1. the exception is fatal, invalidate everything and rethrow, aborting the download
 *          - NEXT: cancel
 *      or
 *      2. update the starting position of the download based on bytes transferred during the latest attempt
 *          - NEXT: resume
 *
 *  - resume: a request for the remaining bytes has been sent. {@link ResumableDownloadHttpRequestInterceptor} will
 *      ask us to update {@code Range} header and when the response object is delivered
 *      {@link ResumableDownloadHttpResponseInterceptor} will ask us to validate response headers before the response
 *      is returned to the user.
 *
 *  - complete: reading of the response body completed successfully.
 *
 *
 * @since 3.2.3
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 */
// @formatter:on
public class ResumableDownloadCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadCoordinator.class);

    /**
     * Key at which the coordinator sets itself into the {@link HttpContext}.
     * Package-private for unit-testing interceptors.
     */
    static final String CTX_RESUMABLE_COORDINATOR = "manta.resumable_coordinator";

    private static final String MESSAGE_FATAL_EXCEPTION =
            "Fatal exception has occurred while attempting to download object content";

    private static final Set<Class<? extends IOException>> EXCEPTIONS_FATAL =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    InterruptedIOException.class,
                                    UnknownHostException.class,
                                    ConnectException.class,
                                    SSLException.class)));

    /**
     * The look-ahead buffering input stream which keeps track of how many bytes have been read from the underlying
     * stream.
     */
    private final ResumableInputStream resumableStream;

    /**
     * We keep a reference to the HttpContext so we can detach from it.
     */
    private final HttpContext context;

    private final AtomicReference<ResumableDownloadMarker> markerHolder;

    /**
     * Hints about the etag and range expected in the {@link ResumableDownloadMarker} which will be attached.
     */
    private final MutablePair<String, HttpRange.Request> markerHint;

    private final Object lock;

    /**
     * Construct a coordinator. Each logical download should use a different coordinator in order to simplify state
     * management. The coordinator requires an {@link HttpContext} so that it can be reached from the related
     * interceptors ({@link ResumableDownloadHttpRequestInterceptor} and
     * {@link ResumableDownloadHttpResponseInterceptor}.
     *
     * @param context the HttpContext with which the request(s) will be executed
     */
    public ResumableDownloadCoordinator(final HttpContext context) {
        this(context, null);
    }

    /**
     * Construct a coordinator. Each logical download should use a different coordinator in order to simplify state
     * management. The coordinator requires an {@link HttpContext} so that it can be reached from the related
     * interceptors ({@link ResumableDownloadHttpRequestInterceptor} and
     * {@link ResumableDownloadHttpResponseInterceptor}.
     *
     * @param context    the HttpContext with which the request(s) will be executed
     * @param bufferSize the size passed to the {@link ResumableInputStream} constructor
     */
    public ResumableDownloadCoordinator(final HttpContext context, final Integer bufferSize) {
        if (null != extractFromContext(context)) {
            throw new IllegalArgumentException("Coordinator already present in context");
        }

        context.setAttribute(CTX_RESUMABLE_COORDINATOR, this);

        final ResumableInputStream stream;
        if (bufferSize != null) {
            stream = new ResumableInputStream(bufferSize);
        } else {
            stream = new ResumableInputStream();
        }

        this.resumableStream = stream;
        this.context = context;
        this.markerHolder = new AtomicReference<>();
        this.markerHint = MutablePair.of(null, null);
        this.lock = new Object();
    }

    public ResumableInputStream getResumableStream() {
        return this.resumableStream;
    }

    boolean inProgress() {
        return this.markerHolder.get() != null;
    }

    void attemptRecovery(final IOException e)
            throws ResumableDownloadUnexpectedResponseException {
        if (EXCEPTIONS_FATAL.contains(e.getClass())) {
            this.cancel();
            throw new ResumableDownloadUnexpectedResponseException(MESSAGE_FATAL_EXCEPTION, e);
        }

        for (final Class<? extends IOException> exceptionClass : EXCEPTIONS_FATAL) {
            if (exceptionClass.isInstance(e)) {
                this.cancel();
                throw new ResumableDownloadUnexpectedResponseException(MESSAGE_FATAL_EXCEPTION, e);
            }
        }

        this.updateMarkerFromStream();
    }

    private void updateMarkerFromStream() {
        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to update");
        }

        marker.updateBytesRead(this.resumableStream.getCount());
    }

    void createMarker(final HttpResponse response)
            throws ResumableDownloadUnexpectedResponseException {
        final ImmutablePair<String, HttpRange.Response> fingerprint = extractResponseFingerprint(response);

        final String initialResponseEtag = fingerprint.left;
        final HttpRange.Response initialResponseContentRange = fingerprint.right;

        synchronized (this.lock) {
            // make sure the first response etag matches (if any was set)
            if (this.markerHint.getLeft() != null
                    && !this.markerHint.getLeft().equals(initialResponseEtag)) {
                this.cancel();
                throw new ResumableDownloadUnexpectedResponseException(
                        String.format(
                                "First response ETag does not satisfy If-Match: expected [%s], got [%s]",
                                this.markerHint.getLeft(),
                                initialResponseEtag));
            }

            // make sure the first response range matches (if any was set)
            if (this.markerHint.getRight() != null
                    && !this.markerHint.getRight().matches(initialResponseContentRange)) {
                this.cancel();
                throw new ResumableDownloadUnexpectedResponseException(
                        String.format(
                                "First response Content-Range does not satisfy request Range: expected [%s], got [%s]",
                                this.markerHint.getRight(),
                                initialResponseContentRange));
            }
        }

        final ResumableDownloadMarker marker =
                new ResumableDownloadMarker(initialResponseEtag, initialResponseContentRange);

        if (!this.markerHolder.compareAndSet(null, marker)) {
            throw new IllegalStateException("Marker already present");
        }

    }

    void prepare(final HttpRequest request)
            throws ResumableDownloadException {
        if (request == null || request.getRequestLine() == null) {
            throw new NullPointerException("Request or request properties are null");
        }

        if (!HttpGet.METHOD_NAME.equalsIgnoreCase(request.getRequestLine().getMethod())) {
            final String message = String.format(
                    "Invalid method in request provided to resume download: expected [GET], got [%s]",
                    request.getRequestLine().getMethod());
            throw new ResumableDownloadIncompatibleRequestException(message);
        }

        // extract the ETag and request range headers (enforcing that 1. they're missing and 2. they are single-valued)
        final ImmutablePair<String, HttpRange.Request> headersOrHints = extractRequestFingerprint(request);

        synchronized (this.lock) {
            final ResumableDownloadMarker marker = this.markerHolder.get();

            // this is the "start" request, pick up hints and proceed normally
            if (marker == null) {
                this.applyHints(headersOrHints);
                return;
            }

            // otherwise this is a "resume" request, update it
            updateResumeRequest(request, headersOrHints, marker);
        }
    }

    private void applyHints(final ImmutablePair<String, HttpRange.Request> hints) throws ResumableDownloadException {
        if (hints.left != null) {
            if (this.markerHint.left != null) {
                throw new ResumableDownloadException("ETag hint already set");
            }
            this.markerHint.setLeft(hints.left);
        }

        if (hints.right != null) {
            if (this.markerHint.right != null) {
                throw new ResumableDownloadException("Range hint already set");
            }
            this.markerHint.setRight(hints.right);
        }
    }

    private static void updateResumeRequest(final HttpRequest request,
                                            final ImmutablePair<String, HttpRange.Request> requestFingerprint,
                                            final ResumableDownloadMarker marker)
            throws ResumableDownloadException {
        notNull(marker, "ResumableDownloadMarker must not be null");

        if (requestFingerprint.left == null) {
            // the request is missing an If-Match header, set it to the known ETag
            request.setHeader(IF_MATCH, marker.getEtag());
        } else if (!requestFingerprint.left.equals(marker.getEtag())) {
            // if the request already has an if-match header but it doesn't match the marker something is wrong
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Incorrect ETag in If-Match header: expected [%s], got [%s]",
                            marker.getEtag(),
                            requestFingerprint.left));
        }

        // unconditionally set the request range to the current range
        request.setHeader(RANGE, marker.getCurrentRange().render());
    }

    /**
     * Extracts a single non-blank header value from the provided request or response, or null if the header wasn't
     * present.
     *
     * @param message    the {@link HttpRequest} or {@link HttpResponse}
     * @param headerName the name of the request/repsonse header
     * @return the single header value if there was only one present, or null if it was missing
     * @throws ResumableDownloadException When the header is present more than once, or is present but blank.
     */
    private static String extractSingleHeaderValue(final HttpMessage message,
                                                   final String headerName,
                                                   final boolean required)
            throws ResumableDownloadException {
        final Header[] headers = message.getHeaders(headerName);

        if (0 == headers.length) {
            if (required) {
                throw new ResumableDownloadException(
                        String.format(
                                "Required [%s] header for resumable downloads missing",
                                headerName));
            }

            return null;
        }

        if (1 < headers.length) {
            throw new ResumableDownloadException(
                    String.format(
                            "Resumable download not compatible with multi-valued [%s] header",
                            headerName));
        }

        // there is a single header
        final Header header = headers[0];

        if (header == null || StringUtils.isBlank(header.getValue())) {
            throw new ResumableDownloadException(
                    String.format("Invalid %s header (blank or missing)", headerName));
        }

        return header.getValue();
    }

    void validate(final HttpResponse response)
            throws ResumableDownloadUnexpectedResponseException {
        notNull(response, "Response must not be null");

        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to compare against response");
        }

        final String etagHeader;
        try {
            etagHeader = extractSingleHeaderValue(response, ETAG, false);
        } catch (final ResumableDownloadException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        // just in case the server ignores if-match
        if (!marker.getEtag().equals(etagHeader)) {
            final String message = String.format(
                    "Invalid ETag header: expected [%s], got [%s]",
                    marker.getEtag(),
                    etagHeader);
            throw new ResumableDownloadUnexpectedResponseException(message);
        }

        final Header contentRangeHeader = response.getFirstHeader(CONTENT_RANGE);

        if (null == contentRangeHeader || StringUtils.isBlank(contentRangeHeader.getValue())) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Invalid Content-Range header (blank or missing)");
        }

        final HttpRange.Response responseRange;
        try {
            responseRange = HttpRange.parseContentRange(contentRangeHeader.getValue());
        } catch (final Exception e) {
            throw new ResumableDownloadUnexpectedResponseException("Invalid Content-Range header (malformed)", e);
        }

        // ask the marker to make sure we got the expected range back
        try {
            marker.validateRange(responseRange);
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Invalid Content-Range for resumed download response", e);
        }
    }

    void cancel() {
        if (this.context.getAttribute(CTX_RESUMABLE_COORDINATOR) != this) {
            throw new IllegalStateException("Coordinator has already been cancelled.");
        }

        this.context.removeAttribute(CTX_RESUMABLE_COORDINATOR);
        this.markerHolder.set(null);

        // TODO: stop throwing quietly or make ResumableInputStream stop closing the backing stream
        IOUtils.closeQuietly(this.resumableStream);
    }

    @Override
    public String toString() {
        return "ResumableDownloadCoordinator{" +
                "resumableStream=" + this.resumableStream +
                ", context=" + this.context +
                ", markerHolder=" + this.markerHolder +
                '}';
    }

    private static ImmutablePair<String, HttpRange.Request> extractRequestFingerprint(final HttpRequest request)
            throws ResumableDownloadIncompatibleRequestException {
        String ifMatch = null;
        HttpRange.Request range = null;

        Exception ifMatchEx = null;
        Exception rangeEx = null;

        try {
            ifMatch = extractSingleHeaderValue(request, IF_MATCH, false);
        } catch (final ResumableDownloadException e) {
            ifMatchEx = e;
        }

        try {
            final String rawRequestRange = extractSingleHeaderValue(request, RANGE, false);
            if (rawRequestRange != null) {
                range = HttpRange.parseRequestRange(rawRequestRange);
            }
        } catch (final ResumableDownloadException | HttpException e) {
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
     * In order to be sure we're continuing to download the same object we need to extract the {@code ETag} and
     * {@code Content-Range} headers from the response. Either header missing is an error. When the
     * {@code Content-Range} header is present the specified range should be equal to the response's
     * {@code Content-Length}.
     *
     * @param response the response to check for headers
     * @return the request headers we're concerned with validating
     * @throws ResumableDownloadUnexpectedResponseException when the headers are malformed, unparseable, or the
     *                                                      {@code Content-Range} and {@code Content-Length} are
     *                                                      mismatched
     */
    private static ImmutablePair<String, HttpRange.Response> extractResponseFingerprint(final HttpResponse response)
            throws ResumableDownloadUnexpectedResponseException {

        final String etag;
        try {
            etag = extractSingleHeaderValue(response, ETAG, true);
        } catch (final ResumableDownloadException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        }

        final long contentLength;
        try {
            final String rawContentLength = extractSingleHeaderValue(response, CONTENT_LENGTH, true);
            contentLength = Long.parseUnsignedLong(notNull(rawContentLength)); // notNull squelches
        } catch (final ResumableDownloadException e) {
            throw new ResumableDownloadUnexpectedResponseException(e);
        } catch (final NumberFormatException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Failed to parse Content-Length response, matching headers: %s",
                            Arrays.deepToString(response.getHeaders(CONTENT_LENGTH))));
        }

        final String rawContentRange;
        try {
            rawContentRange = extractSingleHeaderValue(response, CONTENT_RANGE, false);
        } catch (final ResumableDownloadException e) {
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

        // Manta follows the spec and sends the Content-Length of the range, which we should validate
        if (contentRange.contentLength() != contentLength) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Invalid Content-Length in range response: expected [%d], got [%d]",
                            contentRange.contentLength(),
                            contentLength));
        }

        return new ImmutablePair<>(etag, contentRange);
    }

    static ResumableDownloadCoordinator extractFromContext(final HttpContext ctx) {
        final Object coordinator = ctx.getAttribute(CTX_RESUMABLE_COORDINATOR);

        if (null == coordinator) {
            return null;
        }

        if (!(coordinator instanceof ResumableDownloadCoordinator)) {
            LOG.error("Unexpected type found while extracting resumable coordinator, got: " + coordinator.getClass());
            return null;
        }

        return (ResumableDownloadCoordinator) coordinator;
    }

}
