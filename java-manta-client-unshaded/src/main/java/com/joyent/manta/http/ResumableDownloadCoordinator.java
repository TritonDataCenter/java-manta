package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
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

    private static final Set<Class<? extends IOException>> EXCEPTIONS_FATAL =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    InterruptedIOException.class,
                                    UnknownHostException.class,
                                    ConnectException.class,
                                    SSLException.class)));

    private final ResumableInputStream resumableStream;

    /**
     * We keep a reference to the HttpContext so we can detach from it.
     */
    private final HttpContext context;

    private final AtomicReference<ResumableDownloadMarker> markerHolder;

    /**
     * Hints about the etag and range expected in the {@link ResumableDownloadMarker} which will be attached.
     */
    private MutablePair<String, HttpRange.Request> markerHint;

    private final Object lock;

    public ResumableDownloadCoordinator(final HttpContext context) {
        if (null != extractFromContext(context)) {
            throw new IllegalArgumentException("Coordinator already present in context");
        }

        context.setAttribute(CTX_RESUMABLE_COORDINATOR, this);

        this.resumableStream = new ResumableInputStream();
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

    void attemptRecovery(final IOException e) throws ResumableDownloadUnexpectedResponseException {
        final String message = "Fatal exception has while attempting to download object content";

        if (EXCEPTIONS_FATAL.contains(e.getClass())) {
            this.cancel();
            throw new ResumableDownloadUnexpectedResponseException(message, e);
        }

        for (final Class<? extends IOException> exceptionClass : EXCEPTIONS_FATAL) {
            if (exceptionClass.isInstance(e)) {
                this.cancel();
                throw new ResumableDownloadUnexpectedResponseException(message, e);
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

    void createMarker(final HttpResponse response) throws ResumableDownloadUnexpectedResponseException {
        final ImmutablePair<String, HttpRange.Response> fingerprint = extractResponseFingerprint(response);

        if (fingerprint == null) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Resumed response lacks required headers, aborting retry",
                    response);
        }

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

        if (!this.markerHolder.compareAndSet(null, marker))

        {
            throw new IllegalStateException("Marker already present");
        }

    }

    void enhance(final HttpRequest request) throws ResumableDownloadIncompatibleRequestException {
        if (request == null || request.getRequestLine() == null) {
            throw new NullPointerException("Request or request properties are null");
        }

        if (!HttpGet.METHOD_NAME.equalsIgnoreCase(request.getRequestLine().getMethod())) {
            final String message = String.format(
                    "Invalid method in request provided to resume download, expected: [GET], got [%s]",
                    request.getRequestLine().getMethod());
            throw new ResumableDownloadIncompatibleRequestException(message);
        }

        this.validateExistingRequestHeaders(request);

        final ResumableDownloadMarker marker = this.markerHolder.get();

        if (marker == null) {
            // not yet in progress, we're looking at the first request
            return;
        }

        final Header ifMatchHeader = request.getFirstHeader(IF_MATCH);
        final Header rangeHeader = request.getFirstHeader(RANGE);

        if (ifMatchHeader == null) {
            request.addHeader(IF_MATCH, marker.getEtag());
        } else if (!marker.getEtag().equals(ifMatchHeader.getValue())) {
            // TODO: this should be impossible since the header would've been validated above
            throw new RuntimeException("something impossible has occurred?");
        }

        if (rangeHeader == null) {
            request.addHeader(RANGE, marker.getCurrentRange().render());
        } else {
            throw new NotImplementedException("do the range header needful");
        }
    }


    // TODO: no idea how to break this out onto multiple lines in a satisfatory way..

    /*
    since the following is too long:

    void validateExistingRequestHeaders(final HttpRequest request) throws ResumableDownloadIncompatibleRequestException {

    we need one of:

    void validateExistingRequestHeaders(final HttpRequest request
    ) throws ResumableDownloadIncompatibleRequestException {

        or

    void validateExistingRequestHeaders(
        final HttpRequest request
    ) throws ResumableDownloadIncompatibleRequestException {

        or...?
     */


    private void validateExistingRequestHeaders(final HttpRequest request
    ) throws ResumableDownloadIncompatibleRequestException {
        ResumableDownloadIncompatibleRequestException ifMatchException = null, rangeException = null;

        try {
            this.validateRequestHasCompatibleIfMatchHeader(request);
        } catch (final ResumableDownloadIncompatibleRequestException ifMatchFailed) {
            ifMatchException = ifMatchFailed;
        }

        try {
            this.validateRequestHasCompatibleRangeHeader(request);
        } catch (final ResumableDownloadIncompatibleRequestException rangeFailed) {
            rangeException = rangeFailed;
        }

        if (ifMatchException != null && rangeException != null) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Incompatible Range and If-Match headers for resuming download: %n%s%n%s",
                            rangeException.getMessage(),
                            ifMatchException.getMessage()));
        } else if (ifMatchException != null) {
            throw ifMatchException;
        } else if (rangeException != null) {
            throw rangeException;
        }

        // both were null, everything checks out
    }

    private void validateRequestHasCompatibleRangeHeader(final HttpRequest request
    ) throws ResumableDownloadIncompatibleRequestException {
        final Header[] rangeHeaders = request.getHeaders(RANGE);

        if (1 < rangeHeaders.length) {
            throw new ResumableDownloadIncompatibleRequestException(
                    "Cannot perform resumable download with multi-part Range header");
        }

        if (0 == rangeHeaders.length) {
            return;
        }

        // there is a single range header
        final Header rangeHeader = rangeHeaders[0];

        if (rangeHeader == null || StringUtils.isBlank(rangeHeader.getValue())) {
            throw new ResumableDownloadIncompatibleRequestException("Invalid Range header (blank or missing)");
        }

        final HttpRange.Request requestRange;

        try {
            requestRange = HttpRange.parseRequestRange(rangeHeader.getValue());
        } catch (final HttpException e) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format("Malformed Range header: %s", rangeHeader.getValue()),
                    e);
        }

        final ResumableDownloadMarker marker = this.markerHolder.get();

        if (marker == null) {
            // grab the Range set by the user to we can validate the first response matches
            synchronized (this.lock) {
                if (this.markerHint.getRight() != null) {
                    throw new ResumableDownloadIncompatibleRequestException("Range hint already set");
                }

                this.markerHint.setRight(requestRange);
            }
            return;
        }

        // check against marker
        if (!marker.getCurrentRange().matches(requestRange)) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Range header in Request does not match expected range: expected [%s], got [%s]",
                            marker.getCurrentRange(),
                            requestRange));
        }
    }

    private void validateRequestHasCompatibleIfMatchHeader(final HttpRequest request
    ) throws ResumableDownloadIncompatibleRequestException {
        final Header[] ifMatchHeaders = request.getHeaders(IF_MATCH);

        if (1 < ifMatchHeaders.length) {
            throw new ResumableDownloadIncompatibleRequestException(
                    "Cannot perform resumable download with multi-valued If-Match header");
        }

        if (0 == ifMatchHeaders.length) {
            // no existing if-match header, we'll get the ETag from the first response
            return;
        }

        // there is a single if-match header

        final Header ifMatchHeader = ifMatchHeaders[0];

        if (ifMatchHeader == null || StringUtils.isBlank(ifMatchHeader.getValue())) {
            throw new ResumableDownloadIncompatibleRequestException("Invalid If-Match header (blank or missing)");
        }

        final ResumableDownloadMarker marker = this.markerHolder.get();

        // if we don't have a marker it's the "starting" request
        if (marker == null) {
            // grab the ETag set by the user to we can validate the first response matches
            synchronized (this.lock) {
                if (this.markerHint.getLeft() != null) {
                    throw new ResumableDownloadIncompatibleRequestException("ETag hint already set");
                }

                this.markerHint.setLeft(ifMatchHeader.getValue());
            }

            return;
        }

        // otherwise, this is as "resume" request and we should validate the ETag matches the marker
        if (!marker.getEtag().equals(ifMatchHeader.getValue())) {
            throw new ResumableDownloadIncompatibleRequestException(
                    String.format(
                            "Incorrect ETag in If-Match header: expected [%s], got [%s]",
                            marker.getEtag(),
                            ifMatchHeader.getValue()));
        }
    }


    // TODO: throw ResumableDownloadUnexpectedResponseException instead?
    void validateResponse(final HttpResponse response) throws ResumableDownloadUnexpectedResponseException {
        notNull(response, "Response must not be null");

        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to compare against response");
        }

        final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);

        if (etagHeader == null || StringUtils.isBlank(etagHeader.getValue())) {
            throw new ResumableDownloadUnexpectedResponseException("Invalid ETag header (blank or missing)");
        }

        // just in case the server ignores if-match
        if (!marker.getEtag().equals(etagHeader.getValue())) {
            final String message = String.format(
                    "Invalid ETag header: expected [%s], got [%s]",
                    marker.getEtag(),
                    etagHeader.getValue());
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

        // ask the marker to make sure we got the expected bytes back
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

    private static ImmutablePair<String, HttpRange.Response> extractResponseFingerprint(final HttpResponse response
    ) throws ResumableDownloadUnexpectedResponseException {
        // we can't be sure we're continuing to download the same object if we don't have an etag to compare
        final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);

        if (etagHeader == null) {
            return null;
        }

        final String etag = etagHeader.getValue();

        if (StringUtils.isBlank(etag)) {
            return null;
        }

        final Header contentLengthHeader = response.getFirstHeader(CONTENT_LENGTH);

        if (contentLengthHeader == null) {
            throw new ResumableDownloadUnexpectedResponseException("Response is missing Content-Length header.");
        }

        final long contentLength;
        try {
             contentLength = Long.parseUnsignedLong(contentLengthHeader.getValue());
        } catch (final NumberFormatException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Failed to parse Content-Length response: '%s'", contentLengthHeader.getValue()));
        }

        final HttpRange.Response range;
        final Header rangeHeader = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
        if (rangeHeader == null || StringUtils.isBlank(rangeHeader.getValue())) {
            // the entire object is being requested
            return new ImmutablePair<>(etag, new HttpRange.Response(0, contentLength - 1, contentLength));
        }

        try {
            range = HttpRange.parseContentRange(rangeHeader.getValue());
        } catch (final HttpException e) {
            throw new ResumableDownloadUnexpectedResponseException(
                    "Unable to parse Content-Range header while analyzing download response",
                    e);
        }

        return new ImmutablePair<>(etag, range);
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
