/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Objects;

/**
 * Value only class used to indicate the plaintext positional ranges in which
 * to start reading data from an encrypted stream.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
class PlaintextByteRangePosition {
    /**
     * Initial number of plaintext bytes to skip.
     */
    private long initialPlaintextSkipBytes;

    /**
     * Total length of plaintext to read.
     */
    private long plaintextRangeLength;

    /**
     * The requested plaintext starting position.
     */
    private long plaintextStart;

    /**
     * The requested plaintext ending position.
     */
    private long plaintextEnd;

    /**
     * Creates a new instance.
     */
    PlaintextByteRangePosition() {
    }

    long getInitialPlaintextSkipBytes() {
        return initialPlaintextSkipBytes;
    }

    PlaintextByteRangePosition setInitialPlaintextSkipBytes(
            final long initialPlaintextSkipBytes) {
        this.initialPlaintextSkipBytes = initialPlaintextSkipBytes;
        return this;
    }

    long getPlaintextRangeLength() {
        return plaintextRangeLength;
    }

    PlaintextByteRangePosition setPlaintextRangeLength(
            final long plaintextRangeLength) {
        this.plaintextRangeLength = plaintextRangeLength;
        return this;
    }

    long getPlaintextStart() {
        return plaintextStart;
    }

    PlaintextByteRangePosition setPlaintextStart(final long plaintextStart) {
        this.plaintextStart = plaintextStart;
        return this;
    }

    long getPlaintextEnd() {
        return plaintextEnd;
    }

    PlaintextByteRangePosition setPlaintextEnd(final long plaintextEnd) {
        this.plaintextEnd = plaintextEnd;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlaintextByteRangePosition that = (PlaintextByteRangePosition) o;
        return initialPlaintextSkipBytes == that.initialPlaintextSkipBytes
                && plaintextRangeLength == that.plaintextRangeLength
                && plaintextStart == that.plaintextStart
                && plaintextEnd == that.plaintextEnd;
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialPlaintextSkipBytes, plaintextRangeLength,
                plaintextStart, plaintextEnd);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("initialPlaintextSkipBytes", initialPlaintextSkipBytes)
                .append("plaintextRangeLength", plaintextRangeLength)
                .append("plaintextStart", plaintextStart)
                .append("plaintextEnd", plaintextEnd)
                .toString();
    }
}
