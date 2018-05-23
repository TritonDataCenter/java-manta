package com.joyent.manta.http;

import com.joyent.manta.util.MantaUtils;
import com.joyent.manta.util.MultipartInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Reference;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

class ResumableDownloadMarker implements Closeable {

    private final Object lock;

    /**
     * The etag associated with the object being downloaded. We need to make sure the etag is unchanged between
     * retries.
     */
    private final String etag;

    /**
     * The original download range.
     */
    private final ContentRange target;

    /**
     * The current download range.
     */
    private ContentRange current;

    private final MultipartInputStream multipartInputStream;

    ResumableDownloadMarker(final String etag, final ContentRange range) {
        validState(!StringUtils.isBlank(etag), "ETag must not be null or blank");
        notNull(range, "ContentRange must not be null");

        this.etag = etag;
        this.target = range;
        this.current = this.target;
        this.multipartInputStream = new MultipartInputStream();
        this.lock = new Object();
    }

    public String getEtag() {
        return this.etag;
    }

    public ContentRange getTargetRange() {
        return this.target;
    }

    public void updateStart(final long aNewStart) {
        synchronized (this.lock) {
            this.current = new ContentRange(aNewStart, this.target.end, this.target.size);
        }
    }

    public MultipartInputStream getMultipartInputStream() {
        return this.multipartInputStream;
    }

    @Override
    public void close() throws IOException {
        this.multipartInputStream.close();
    }

    static ImmutablePair<String, ContentRange> extractFingerprint(final HttpResponse response) {

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

        if (0 < entity.getContentLength()) {
            // either this is a zero byte entity, the content length is unknown (-1), or it's larger than 9.2 exabytes
            // (in which case we'll need to revisit how incremental downloads would work at that scale)
            return null;
        }

        final ContentRange range;
        final Header rangeHeader = response.getFirstHeader(HttpHeaders.CONTENT_RANGE);
        if (rangeHeader == null || StringUtils.isBlank(rangeHeader.getValue())) {
            // the entire object is being requested
            range = new ContentRange(0, contentLength - 1, contentLength);
        } else {
            range = ContentRange.parse(rangeHeader.getValue());
        }

        return new ImmutablePair<>(etag, range);
    }
}
