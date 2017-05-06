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
 * Class describing a manta object byte range (open).
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class OpenByteRange implements OpenRange, ByteRange<OpenByteRange> {

    /**
    * Start bound (inclusive).
    */
    private final long start;

    /**
     * {@code OpenByteRange} constructor.
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     *
     * @throws IllegalArgumentException if {@code start} is invalid
     */
    public OpenByteRange(final long start) {

        validate(start);

        this.start = start;
    }

    @Override
    public long getStart() {

        return start;
    }

    @Override
    public Optional<Long> getLength() {

        if (start < 0) {

            return Optional.of(-start);

        } else {

            return Optional.empty();
        }

    }

    @Override
    public OpenByteRange doFix(final long length) {

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

        if (start < -length) {

            return new OpenByteRange(-length);

        } else {

            return new OpenByteRange(start);
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

        OpenByteRange ob = (OpenByteRange) o;

        return this.start == ob.start;
    }

    @Override
    public int hashCode() {

        return Objects.hash(start);
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .append("start", start)
                .toString();
    }

    /**
     * Method to validate byte range bounds (start).
     *
     * @param start start bound
     *
     * @throws IllegalArgumentException if {@code start} is invalid
     */
    private static void validate(final long start) {

        assert (start != 0);
        assert ((start < Long.MAX_VALUE) && (start > Long.MIN_VALUE));

        if (start == 0)  {

            throw new IllegalArgumentException("start must be not be zero");
        }

        if ((start == Long.MAX_VALUE || start == Long.MIN_VALUE))  {

            final String msg = "start must be between Long.MIN_VALUE["
                    + Long.MIN_VALUE + "] and Long.MAX_VALUE["
                    + Long.MAX_VALUE + "] (start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

    }

}
