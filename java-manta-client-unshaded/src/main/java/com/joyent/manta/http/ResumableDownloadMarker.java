/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpException;

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.Validate.validState;

final class ResumableDownloadMarker {

    /**
     * The ETag associated with the object being downloaded. We need to make sure the ETag is unchanged between
     * retries.
     */
    private final String etag;

    /**
     * The current download range.
     */
    private HttpRange.Request currentRange;

    private final long previousStartInclusive;

    private final long totalRangeSize;

    ResumableDownloadMarker(final String etag,
                            final HttpRange.Response initialContentRange) {
        validState(StringUtils.isNotBlank(etag), "ETag must not be null or blank");
        notNull(initialContentRange, "HttpRange must not be null");

        this.etag = etag;
        this.currentRange = new HttpRange.Request(
                initialContentRange.getStartInclusive(),
                initialContentRange.getEndInclusive());
        this.previousStartInclusive = initialContentRange.getStartInclusive();
        this.totalRangeSize = initialContentRange.getSize();
    }

    String getEtag() {
        return this.etag;
    }

    HttpRange.Request getCurrentRange() {
        return this.currentRange;
    }

    void updateBytesRead(final long bytesRead) {
        final long nextStartInclusive = this.currentRange.getStartInclusive() + bytesRead;

        // bytesRead must be:
        validState(
                // 1. non-negative
                // 2. equal to or greater than the previous number of bytes read
                // 3. less than or equal to the expected total number of bytes
                0 <= bytesRead && bytesRead <= this.totalRangeSize && this.previousStartInclusive <= nextStartInclusive,
                "Resumed download range-start should be equal to or greater than current Request Range");

        this.currentRange = new HttpRange.Request(
                nextStartInclusive,
                this.currentRange.getEndInclusive());
    }

    void validateRange(final HttpRange.Response responseRange) throws HttpException {
        if (!this.currentRange.matches(responseRange)) {
            throw new HttpException(
                    String.format(
                            "Content-Range does not match goal range: expected: [%d-%d], got [%d-%d]",
                            this.currentRange.startInclusive,
                            this.currentRange.endInclusive,
                            responseRange.startInclusive,
                            responseRange.endInclusive));
        }
    }

    @Override
    public String toString() {
        return "ResumableDownloadMarker{" +
                "etag='" + this.etag + '\'' +
                ", currentRange=" + this.currentRange +
                '}';
    }
}
