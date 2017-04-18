/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.joyent.manta.client.AdjustableRange;
import com.joyent.manta.client.ClosedRange;
import com.joyent.manta.client.FixableRange;
import com.joyent.manta.client.NullRange;
import com.joyent.manta.client.OpenRange;

/**
 * Class describing a manta http object byte range decorator (closed).
 *
 * @param <T> type of a decorated {@code NullRange}
 * @param <U> type of a decorated {@code OpenRange}
 * @param <V> type of decorated {@code ClosedRange}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class MantaHttpClosedByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                      U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                      V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>>
       implements ClosedRange,
                  MantaHttpByteRange<T, U, V, V> {

    /**
     * Range to add manta http functionality to.
     */
    private final V range;

    /**
     * {@code MantaHttpClosedByteRange} constructor.
     *
     * @param range range to add manta http functionality to
     */
    public MantaHttpClosedByteRange(final V range) {

        validate(range);

        this.range = range;
    }

    @Override
    public long getStart() {

        return range.getStart();
    }

    @Override
    public long getEnd() {

        return range.getEnd();
    }

    @Override
    public Optional<Long> getLength() {

        return range.getLength();
    }

    @Override
    public void addTo(final MantaHttpHeaders headers) {

        final String byteRange;

        final String units = MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT;

        final long start = range.getStart();
        final long end = range.getEnd();

        byteRange = String.valueOf(start) + "-" + String.valueOf(end - 1);

        headers.setRange(units + "=" + byteRange);
    }

    @Override
    public MantaHttpClosedByteRange<T, U, V> doFix(final long length) {

        final long start = getStart();

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

        V fixed = range.doFix(length);

        return new MantaHttpClosedByteRange<T, U, V>(fixed);
    }

    @Override
    public MantaHttpNullByteRange<T, U, V> doAdjust() {

        T adjusted = range.doAdjust();

        return new MantaHttpNullByteRange<T, U, V>(adjusted);
    }

    @Override
    public MantaHttpOpenByteRange<T, U, V> doAdjust(final long start) {

        U adjusted = range.doAdjust(start);

        return new MantaHttpOpenByteRange<T, U, V>(adjusted);
    }

    @Override
    public MantaHttpClosedByteRange<T, U, V> doAdjust(final long start, final long end) {

        V adjusted = range.doAdjust(start, end);

        return new MantaHttpClosedByteRange<T, U, V>(adjusted);
    }

    @Override
    public V doUndecorate() {

        return range;
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MantaHttpClosedByteRange<?, ?, ?> mcb = (MantaHttpClosedByteRange<?, ?, ?>) o;

        return range.equals(mcb.range);
    }

    @Override
    public int hashCode() {

        return Objects.hash(range);
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .append("range", range)
                .toString();
    }

    /**
     * Method to validate a range.
     *
     * @param range range
     *
     * @throws IllegalArgumentException if {@code range} is invalid
     */
    private static void validate(final ClosedRange range) {

        assert (range != null);
        assert ((range.getStart() >= 0) && (range.getEnd() >= 0));

        if (range == null) {

            throw new IllegalArgumentException("range must not be null");
        }

        final long start = range.getStart();
        final long end = range.getEnd();

        if ((start < 0) || (end < 0)) {

            final String msg = "start and end must be positive [RFC 7233] (start="
                    + start + ", end="
                    + end + ")";
            throw new IllegalArgumentException(msg);
        }

    }

}
