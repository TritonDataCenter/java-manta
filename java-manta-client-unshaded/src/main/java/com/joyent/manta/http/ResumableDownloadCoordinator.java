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

    private final AtomicReference<HttpContext> contextHolder;

    private final AtomicReference<ResumableDownloadMarker> markerHolder;


    ResumableDownloadCoordinator(final ResumableInputStream resumableStream,
                                 final HttpContext context) {
        if (null != extractFromContext(context)) {
            throw new IllegalArgumentException("Coordinator already present in context");
        }

        context.setAttribute(CTX_RESUMABLE_COORDINATOR, this);

        this.resumableStream = resumableStream;
        this.contextHolder = new AtomicReference<>(context);
        this.markerHolder = new AtomicReference<>();
        this.lock = new Object();
    }

    // @Override
    // public void close() throws IOException {
    //     this.markerHolder.set(null);
    // }

    boolean canStart() {
        final boolean contextPresent, markerMissing;
        synchronized (this.lock) {
            contextPresent = this.contextHolder.get() != null;
            markerMissing = this.markerHolder.get() == null;
        }

        return contextPresent && markerMissing;
    }

    boolean canResume() {
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

    void updateMarker() {
        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to update");
        }

        marker.updateStart(this.resumableStream.getCount());
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
        notNull(request);

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

    void validateResponse(final HttpResponse response) throws MantaResumedDownloadIncompatibleResponseException {
        notNull(response);
        final ResumableDownloadMarker marker = this.markerHolder.get();
        if (null == marker) {
            throw new IllegalStateException("No marker to compare against response");
        }

        final Header contentRangeHeader = response.getFirstHeader(CONTENT_RANGE);

        if (null == contentRangeHeader || StringUtils.isBlank(contentRangeHeader.getValue())) {
            throw new IllegalArgumentException("Invalid Content-Range header (blank or missing)");
        }

        final HttpRange range;
        try {
            range = HttpRange.parseContentRange(contentRangeHeader.getValue());
        } catch (final Exception e) {
            throw new IllegalArgumentException("Invalid Content-Range header (malformed)", e);
        }

        if (!range.equals(marker.getCurrentRange())) {
            final StringBuilder message = new StringBuilder();
            message.append("Response Content-Range does not match range request, expected: [");
            message.append(marker.getCurrentRange());
            message.append("], received: [");
            message.append(range);
            message.append("]");

            throw new MantaResumedDownloadIncompatibleResponseException(message.toString());
        }
    }

    void cancel() {
        final HttpContext ctx = contextHolder.get();
        if (null == ctx) {
            throw new IllegalStateException("No know HttpContext from which to detach");
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

    static boolean isRecoverable(final IOException e) {
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

    static ImmutablePair<String, HttpRange> extractFingerprint(final HttpResponse response) {

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

        final HttpRange range;
        final Header rangeHeader = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
        if (rangeHeader == null || StringUtils.isBlank(rangeHeader.getValue())) {
            // the entire object is being requested
            range = new HttpRange(0, contentLength - 1, contentLength);
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
