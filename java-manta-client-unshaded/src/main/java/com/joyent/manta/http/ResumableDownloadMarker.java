package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

class ResumableDownloadMarker {

    /**
     * The etag associated with the object being downloaded. We need to make sure the etag is unchanged between
     * retries.
     */
    private final String etag;

    /**
     * The original download range.
     */
    private final HttpRange target;

    /**
     * The current download range.
     */
    private HttpRange current;

    ResumableDownloadMarker(final String etag,
                            final HttpRange range) {
        validState(StringUtils.isNotBlank(etag), "ETag must not be null or blank");
        notNull(range, "HttpRange must not be null");

        this.etag = etag;
        this.target = range;
        this.current = this.target;
    }

    public String getEtag() {
        return this.etag;
    }

    public HttpRange getTargetRange() {
        return this.target;
    }

    public void updateStart(final long aNewStart) {
        this.current = new HttpRange(aNewStart, this.target.end, this.target.size);
    }

    public HttpRange getCurrentRange() {
        return this.current;
    }

    @Override
    public String toString() {
        return "ResumableDownloadMarker{" +
                "etag='" + this.etag + '\'' +
                ", target=" + this.target +
                ", current=" + this.current +
                '}';
    }
}
