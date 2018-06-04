package com.joyent.manta.http;

import com.joyent.manta.exception.MantaResumedDownloadIncompatibleResponseException;
import com.joyent.manta.util.ResumableInputStream;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
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
import static org.apache.commons.lang3.Validate.validState;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;

// @formatter:off
/**
 *
 * Manages state needed to "resume" a download by updating the {@code Range} header in subsequent retries.
 * States:
 *  - ready: freshly constructed, registers self into HttpContext
 *      - NEXT: fatalExit, started
 *  - fatalExit: at any point while copying a response body an exception may be caught and
 *  {@link #attemptRecovery (IOException)} will indicate if the download should be abandoned.
 *
 *
 *  - started: attachMarker called after first response has arrived, recording the "goal" range we intend to satisfy
 *      - NEXT: complete
 *
 *  TODO: finish state listing/transitions
 *
 * @since 3.2.3
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 */
// @formatter:on
public class ResumableDownloadCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadCoordinator.class);

    private static final String CTX_RESUMABLE_COORDINATOR = "manta.resumable_coordinator";

    private final Object lock;

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
    private final AtomicReference<HttpContext> contextHolder;

    private final AtomicReference<ResumableDownloadMarker> markerHolder;

    ResumableDownloadCoordinator(final HttpContext context) {
        if (null != extractFromContext(context)) {
            throw new IllegalArgumentException("Coordinator already present in context");
        }

        context.setAttribute(CTX_RESUMABLE_COORDINATOR, this);

        this.resumableStream = new ResumableInputStream();
        this.contextHolder = new AtomicReference<>(context);
        this.markerHolder = new AtomicReference<>();
        this.lock = new Object();
    }

    public ResumableInputStream getResumableStream() {
        return this.resumableStream;
    }

    boolean inProgress() {
        final boolean contextPresent, markerPresent;
        synchronized (this.lock) {
            contextPresent = this.contextHolder.get() != null;
            markerPresent = this.markerHolder.get() != null;
        }

        return contextPresent && markerPresent;
    }

    void attachMarker(final ResumableDownloadMarker marker) {
        if (!this.markerHolder.compareAndSet(null, marker)) {
            throw new IllegalStateException("Marker already present");
        }
    }

    private void updateMarkerFromStream() {
        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to update");
        }

        marker.updateRequestStart(this.resumableStream.getCount());
    }

    boolean requestHasCompatibleRangeHeaders(final HttpRequest request) {
        final Header[] rangeHeaders = request.getHeaders(IF_MATCH);

        if (1 < rangeHeaders.length) {
            return false;
        }

        if (0 == rangeHeaders.length) {
            return true;
        }

        // there is a single range header
        // TODO: we'll need to figure out how to handle pre-existing ranges and more than one retry
        throw new NotImplementedException("gotta get here someday!");
    }

    boolean requestHasCompatibleIfMatchHeaders(final HttpRequest request) {
        final Header[] ifMatchHeaders = request.getHeaders(IF_MATCH);

        if (1 < ifMatchHeaders.length) {
            return false;
        }

        if (0 == ifMatchHeaders.length) {
            return true;
        }

        // there is a single if-match header
        // if we have a marker it has to match,
        // otherwise, this is the first request and the user has specified their own if-match, which is fine

        final ResumableDownloadMarker marker = this.markerHolder.get();

        if (null == marker) {
            return true;
        }

        final Header ifMatchHeader = ifMatchHeaders[0];
        return marker.getEtag().equals(ifMatchHeader.getValue());
    }

    void applyHeaders(final HttpRequest request) {
        if (request == null || request.getRequestLine() == null) {
            throw new NullPointerException("Request or request properties are null");
        }

        if (!HttpGet.METHOD_NAME.equalsIgnoreCase(request.getRequestLine().getMethod())) {
            final String message = String.format(
                    "Invalid request provided to resume download, expected: [GET], got [%s]",
                    request.getRequestLine().getMethod());
            throw new IllegalArgumentException(message);
        }

        final ResumableDownloadMarker marker = notNull(this.markerHolder.get());

        final Header ifMatchHeader = request.getFirstHeader(IF_MATCH);
        final Header rangeHeader = request.getFirstHeader(RANGE);

        LOG.debug("applying headers");

        if (ifMatchHeader == null) {
            LOG.debug("added if-match");
            request.addHeader(IF_MATCH, marker.getEtag());
        } else {
            LOG.debug("comparing if-match");
            validState(marker.getEtag().equals(ifMatchHeader.getValue()));
            LOG.debug("valid if-match");
        }

        if (rangeHeader == null) {
            LOG.debug("adding range");
            request.addHeader(RANGE, marker.getCurrentRange().renderRequestRange());
        } else {
            LOG.debug("comparing or updating range");
            throw new NotImplementedException("do the range header needful");
        }
    }

    // TODO: throw MantaResumedDownloadIncompatibleResponseException instead?
    void validateResponse(final HttpResponse response) throws MantaResumedDownloadIncompatibleResponseException {
        notNull(response);
        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to compare against response");
        }

        if (null == marker.getCurrentRange()) {
            throw new IllegalStateException("No request in marker to compare against response");
        }


        final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);

        if (etagHeader == null || StringUtils.isBlank(etagHeader.getValue())) {
            throw new IllegalArgumentException("Invalid ETag header (blank or missing)");
        }

        // just in case the server ignores if-match
        if (!marker.getEtag().equals(etagHeader.getValue())) {
            final String message = String.format(
                    "Invalid ETag header: expected [%s], got [%s]",
                    marker.getEtag(),
                    etagHeader.getValue());
            throw new IllegalArgumentException(message);
        }

        final Header contentRangeHeader = response.getFirstHeader(CONTENT_RANGE);

        if (null == contentRangeHeader || StringUtils.isBlank(contentRangeHeader.getValue())) {
            throw new IllegalArgumentException("Invalid Content-Range header (blank or missing)");
        }

        final HttpRange.Response responseRange;
        try {
            responseRange = HttpRange.parseContentRange(contentRangeHeader.getValue());
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid Content-Range header (malformed)", e);
        }



        // as the marker to make sure we got the expected bytes back
        marker.validateResponseRange(responseRange);
    }

    void cancel() {
        final HttpContext ctx = contextHolder.get();
        if (null == ctx) {
            throw new IllegalStateException("No HttpContext from which to detach");
        }

        ctx.removeAttribute(CTX_RESUMABLE_COORDINATOR);
        contextHolder.set(null);
        markerHolder.set(null);
    }

    @Override
    public String toString() {
        return "ResumableDownloadCoordinator{" +
                "resumableStream=" + this.resumableStream +
                ", contextHolder=" + this.contextHolder +
                ", markerHolder=" + this.markerHolder +
                '}';
    }

    void attemptRecovery(final IOException e) throws MantaResumedDownloadIncompatibleResponseException {
        final String message = "Fatal exception has while attempting to download object content";

        if (EXCEPTIONS_FATAL.contains(e.getClass())) {
            throw new MantaResumedDownloadIncompatibleResponseException(message, e);
        }

        for (final Class<? extends IOException> exceptionClass : EXCEPTIONS_FATAL) {
            if (exceptionClass.isInstance(e)) {
                throw new MantaResumedDownloadIncompatibleResponseException(message, e);
            }
        }

        this.updateMarkerFromStream();
    }

    static ImmutablePair<String, HttpRange.Response> extractResponseFingerprint(final HttpResponse response) {
        // we can't be sure we're continuing to download the same object if we don't have an etag to compare
        final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);

        if (etagHeader == null) {
            return null;
        }

        final String etag = etagHeader.getValue();

        if (StringUtils.isBlank(etag)) {
            return null;
        }

        final HttpEntity entity = response.getEntity();

        if (entity == null) {
            // no response body?
            return null;
        }

        final long contentLength = entity.getContentLength();

        if (entity.getContentLength() < 0) {
            // either this is a zero byte entity, the content length is unknown (-1), or it's larger than 9.2 exabytes
            // (in which case we'll need to revisit how incremental downloads would work at that scale)
            return null;
        }

        final HttpRange.Response range;
        final Header rangeHeader = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
        if (rangeHeader == null || StringUtils.isBlank(rangeHeader.getValue())) {
            // the entire object is being requested
            range = new HttpRange.Response(0, contentLength - 1, contentLength);
        } else {
            range = HttpRange.parseContentRange(rangeHeader.getValue());
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
