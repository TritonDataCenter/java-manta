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
    private final HttpRange.Goal goalRange;

    /**
     * The current download range.
     */
    private HttpRange.Request currentRequestRange;

    ResumableDownloadMarker(final String etag,
                            final HttpRange.Response initialContentRange) {
        validState(StringUtils.isNotBlank(etag), "ETag must not be null or blank");
        notNull(initialContentRange, "HttpRange must not be null");

        this.etag = etag;
        this.goalRange = new HttpRange.Goal(
                initialContentRange.getStartInclusive(),
                initialContentRange.getEndInclusive(),
                initialContentRange.getSize());
        this.currentRequestRange = null;
    }

    String getEtag() {
        return this.etag;
    }

    HttpRange.Request getCurrentRange() {
        return this.currentRequestRange;
    }

    void updateRequestStart(final long startInclusive) {
        // check that the currentRequestRange byte stream is actually a continuation of the previous stream
        // TODO: should be lte or just lt?
        validState(this.goalRange.getStartInclusive() <= startInclusive, "Resumed download range-start should be equal to or greater than currentRequestRange request");

        this.currentRequestRange = new HttpRange.Request(startInclusive, this.goalRange.getEndInclusive());
    }

    void validateResponseRange(final HttpRange.Response responseRange) {
        if (this.currentRequestRange == null) {
            throw new NullPointerException("No request range present to compare against response range.");
        }

        this.currentRequestRange.validateResponseRange(responseRange);
        this.goalRange.validateResponseRange(responseRange);
    }

    @Override
    public String toString() {
        return "ResumableDownloadMarker{" +
                "etag='" + this.etag + '\'' +
                ", goalRange=" + this.goalRange +
                ", currentRequestRange=" + this.currentRequestRange +
                '}';
    }
}
