/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

/**
 *
 */
final class ResumableDownloadMarker {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadMarker.class);

    /**
     * The ETag associated with the object being downloaded. We need to make sure the ETag is unchanged between
     * retries.
     */
    private final String etag;

    /**
     * The starting offset of the initial request/response. Used as part of verifying that the next range will not lie
     * outside of the target request range. We need to keep this in addition to {@link #previousStartInclusive} because
     * the number of bytes provided to {@link #updateRangeStart(long)} is relative to the
     * <strong>initial</strong> start offset, not the latest start offset.
     */
    private final long originalRangeStart;

    /**
     * The total size of the target request range for Range requests, otherwise this is the Content-Length.
     */
    private final long totalRangeSize;

    /**
     * The previous start of range. Similar to {@link #originalRangeStart} but only remembers the latest value.
     */
    private long previousStartInclusive;

    /**
     * The current download range.
     */
    private HttpRange.Request currentRange;

    /**
     * Build a marker from the initial ETag and response range. The Response range may be constructed from a singular
     * Content-Length or from a Content-Range header.
     *
     * @param etag
     * @param initialContentRange
     */
    private ResumableDownloadMarker(final String etag,
                                    final HttpRange.Response initialContentRange) {
        validState(StringUtils.isNotBlank(etag), "ETag must not be null or blank");
        notNull(initialContentRange, "HttpRange must not be null");

        this.etag = etag;
        this.previousStartInclusive = initialContentRange.getStartInclusive();
        this.originalRangeStart = this.previousStartInclusive;
        this.totalRangeSize = initialContentRange.getSize();
        this.currentRange = new HttpRange.Request(
                this.previousStartInclusive,
                initialContentRange.getEndInclusive());
    }

    String getEtag() {
        return this.etag;
    }

    HttpRange.Request getCurrentRange() {
        return this.currentRange;
    }

    /**
     * Advance the marker's state, updating {@link #currentRange}. Verifies that the next starting offset: 1.
     * non-negative (zero is acceptable because the user may have not read any bytes into the initial request) 2. less
     * than the previous start of the range (the user can't "unread" bytes and move backwards, {@link
     * InputStream#reset()} is not supported by resumable downloads) 3. less than the total number of bytes we expected
     * the user to read (where would those bytes come from?) 4. less than or equal to the end of the target range, this
     * is a restatement of 3 but checks our own math
     *
     * @param totalBytesRead number of bytes read across all resumed responses
     */
    void updateRangeStart(final long totalBytesRead) {
        final long nextStartInclusive = this.originalRangeStart + totalBytesRead;

        if (totalBytesRead < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Bytes read [%d] cannot be negative",
                            totalBytesRead));
        }

        if (nextStartInclusive < this.previousStartInclusive) {
            throw new IllegalArgumentException(
                    String.format(
                            "Next start position [%d] cannot decrease, previously [%d]",
                            nextStartInclusive,
                            this.previousStartInclusive));
        }

        if (this.totalRangeSize < totalBytesRead) {
            throw new IllegalArgumentException(
                    String.format(
                            "Bytes read [%d] cannot be greater than expected number of bytes [%d]",
                            totalBytesRead,
                            this.totalRangeSize));
        }

        if (this.currentRange.getEndInclusive() < nextStartInclusive) {
            throw new IllegalArgumentException(
                    String.format(
                            "Next start position [%d] cannot be greater than end of range [%d]",
                            nextStartInclusive,
                            this.currentRange.getEndInclusive()));
        }

        LOG.debug("Constructing range: {} - {}", nextStartInclusive, this.currentRange.getEndInclusive());

        final HttpRange.Request nextRange;
        try {
            nextRange = new HttpRange.Request(nextStartInclusive, this.currentRange.getEndInclusive());
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException(e);
        }

        this.previousStartInclusive = this.currentRange.getStartInclusive();
        this.currentRange = nextRange;
    }

    /**
     * Verify that the Content-Range returned by a request matches the Range header that was sent. Because a {@link
     * HttpRange.Request} does not contain a total object size, only the start and end offsets should be checked.
     *
     * @param responseRange the parsed Content-Range header as a {@link HttpRange.Response}
     * @throws HttpException in case the returned range does not match, this should've been a (416) response
     */
    void validateResponseRange(final HttpRange.Response responseRange) throws HttpException {
        if (!this.currentRange.matches(responseRange)) {
            throw new HttpException(
                    String.format(
                            "Content-Range does not match goal range: expected: [%d-%d], got [%d-%d]",
                            this.currentRange.getStartInclusive(),
                            this.currentRange.getEndInclusive(),
                            responseRange.getStartInclusive(),
                            responseRange.getEndInclusive()));
        }
    }

    /**
     * Confirm that the initial response matches either of the hints that the initial request supplied. If a hint is
     * missing it will not be checked.
     *
     * @param requestHints etag and range from initial request (If-Match and Range)
     * @param responseFingerprint etag and range from initial request (ETag and Content-Range)
     * @return a marker which can be used to verify future requests
     * @throws ResumableDownloadUnexpectedResponseException thrown when a hint is provided but not satisfied
     */
    static ResumableDownloadMarker validateInitialExchange(final Pair<String, HttpRange.Request> requestHints,
                                                           final Pair<String, HttpRange.Response> responseFingerprint)
            throws ResumableDownloadUnexpectedResponseException {

        if (requestHints.getLeft() != null && !requestHints.getLeft().equals(responseFingerprint.getLeft())) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "ETag does not match If-Match: If-Match [%s], ETag [%s]",
                            requestHints.getLeft(),
                            responseFingerprint.getLeft()));

        }

        if (requestHints.getRight() != null && !requestHints.getRight().matches(responseFingerprint.getRight())) {
            throw new ResumableDownloadUnexpectedResponseException(
                    String.format(
                            "Content-Range does not match Request range: Range [%s], Content-Range [%s]",
                            requestHints.getRight(),
                            responseFingerprint.getRight()));
        }

        return new ResumableDownloadMarker(responseFingerprint.getLeft(), responseFingerprint.getRight());
    }

    @Override
    public String toString() {
        return "ResumableDownloadMarker{"
                + "etag='" + this.etag + '\''
                + ", originalRangeStart=" + this.originalRangeStart
                + ", totalRangeSize=" + this.totalRangeSize
                + ", previousStartInclusive=" + this.previousStartInclusive
                + ", currentRange=" + this.currentRange
                + '}';
    }
}
