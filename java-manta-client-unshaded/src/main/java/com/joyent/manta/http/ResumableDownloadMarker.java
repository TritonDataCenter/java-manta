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

class ResumableDownloadMarker {

    /**
     * The ETag associated with the object being downloaded. We need to make sure the ETag is unchanged between
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
        this.currentRequestRange = new HttpRange.Request(
                initialContentRange.getStartInclusive(),
                initialContentRange.getEndInclusive());
    }

    String getEtag() {
        return this.etag;
    }

    HttpRange.Request getCurrentRange() {
        return this.currentRequestRange;
    }

    void updateBytesRead(final long startInclusive) {
        // check that the currentRequestRange byte stream is actually a continuation of the previous stream
        // TODO: should be lte or just lt?
        validState(
                this.goalRange.getStartInclusive() <= startInclusive,
                "Resumed download range-start should be equal to or greater than current Request Range");

        this.currentRequestRange = new HttpRange.Request(startInclusive, this.goalRange.getEndInclusive());
    }

    void validateRange(final HttpRange.Response responseRange) throws HttpException {
        if (!this.currentRequestRange.matches(responseRange)) {
            throw new HttpException(
                    String.format(
                            "Content-Range does not match goal range: expected: [%d-%d], got [%d-%d]",
                            this.currentRequestRange.startInclusive,
                            this.currentRequestRange.endInclusive,
                            responseRange.startInclusive,
                            responseRange.endInclusive));
        }

        if (!this.goalRange.matches(responseRange)) {
            throw new HttpException(
                    String.format(
                            "Content-Range does not match goal range end or size: expected: [%s], got [%s]",
                            this.goalRange,
                            responseRange));
        }
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
