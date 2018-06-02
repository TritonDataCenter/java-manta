package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;

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
    private final HttpRange.Goal goal;

    /**
     * The currentRequest download range.
     */
    private HttpRange.Request currentRequest;

    ResumableDownloadMarker(final String etag,
                            final HttpRange.Response initialContentRange) {
        validState(StringUtils.isNotBlank(etag), "ETag must not be null or blank");
        notNull(initialContentRange, "HttpRange must not be null");

        this.etag = etag;
        this.goal = new HttpRange.Goal(
                initialContentRange.getStartInclusive(),
                initialContentRange.getEndInclusive(),
                initialContentRange.getSize());
        this.currentRequest = null;
    }

    public String getEtag() {
        return this.etag;
    }

    public HttpRange getTargetRange() {
        return this.goal;
    }

    public void updateRequestStart(final long startInclusive) {
        // check that the currentRequest byte stream is actually a continuation of the previous stream
        // TODO: should be lte or just lt?
        validState(this.goal.getStartInclusive() <= startInclusive, "Resumed download range-start should be equal to or greater than currentRequest request");

        this.currentRequest = new HttpRange.Request(startInclusive, this.goal.getEndInclusive());
    }

    public HttpRange getCurrentRange() {
        return this.currentRequest;
    }

    @Override
    public String toString() {
        return "ResumableDownloadMarker{" +
                "etag='" + this.etag + '\'' +
                ", goal=" + this.goal +
                ", currentRequest=" + this.currentRequest +
                '}';
    }
}
