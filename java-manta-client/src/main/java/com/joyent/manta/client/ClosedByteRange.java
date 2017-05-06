/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Class describing a manta object byte range (closed).
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class ClosedByteRange implements ClosedRange, ByteRange<ClosedByteRange> {

    /**
    * Start bound (inclusive).
    */
    private final long start;

    /**
    * End bound (exclusive).
    */
    private final long end;

    /**
     * {@code ClosedByteRange} constructor.
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     * @param end end bound (exclusive negative interpreted as end offset)
     *
     * @throws IllegalArgumentException if start or end are invalid
     */
    public ClosedByteRange(final long start, final long end) {

        validate(start, end);

        this.start = start;
        this.end = end;
    }

    @Override
    public long getStart() {

        return start;
    }

    @Override
    public long getEnd() {

        return end;
    }

    @Override
    public Optional<Long> getLength() {

        return Optional.of(end - start);
    }

    @Override
    public ClosedByteRange doFix(final long length) {

        assert (length > 0);
        assert (start < length);

        if (length <= 0) {

            final String msg = "length must be greater than zero (length=" + length + ")";
            throw new IllegalArgumentException(msg);
        }

        if (length <= start) {

            final String msg = "length must be greater than start (length=" + length + ", start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

        if (end > length) {

            return new ClosedByteRange(start, length);

        } else {

            return new ClosedByteRange(start, end);
        }

    }

    @Override
    public NullByteRange doAdjust() {

        return new NullByteRange();
    }

    @Override
    public OpenByteRange doAdjust(final long aStart) {

        return new OpenByteRange(aStart);
    }

    @Override
    public ClosedByteRange doAdjust(final long aStart, final long aEnd) {

        return new ClosedByteRange(aStart, aEnd);
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClosedByteRange cb = (ClosedByteRange) o;

        return ((start == cb.start) && (end == cb.end));
    }

    @Override
    public int hashCode() {

        return Objects.hash(start, end);
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .append("start", start)
                .append("end", end)
                .toString();
    }

    /**
     * Method to validate byte range bounds (start, end).
     *
     * @param start start bound
     * @param end end bound
     *
     * @throws IllegalArgumentException if start, and or end are invalid
     */
    private static void validate(final long start, final long end) {

        assert ((start >= -Long.MAX_VALUE) && (start < Long.MAX_VALUE));
        assert (end > -Long.MAX_VALUE);

        assert (start < end);
        assert ((Math.min(0, Math.signum(end)) - Math.min(0, Math.signum(start))) == 0);

        if ((start >= Long.MAX_VALUE) || (start < -Long.MAX_VALUE)) {

            final String msg = "start must be within ["
                    + -Long.MAX_VALUE + ", "
                    + Long.MAX_VALUE + ") (start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

        if (end <= -Long.MAX_VALUE) {

            final String msg = "end must be within ("
                    + -Long.MAX_VALUE + ", "
                    + Long.MAX_VALUE + "] (end=" + end + ")";
            throw new IllegalArgumentException(msg);
        }

        if (start >= end)  {

            final String msg = "start must precede end (start="
                    + start + ", end=" + end + ")";
            throw new IllegalArgumentException(msg);
        }

        if ((Math.min(0, Math.signum(end)) - Math.min(0, Math.signum(start))) != 0) {

            final String msg = "start and end must be closed... offset from same side (start="
                    + start + ", end=" + end + ")";
            throw new IllegalArgumentException(msg);
        }

    }

}
