/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Class describing a manta object byte range (null).
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class NullByteRange implements NullRange, ByteRange<NullByteRange> {

    /**
     * {@code NullByteRange} constructor.
     */
    public NullByteRange() { }

    @Override
    public Optional<Long> getLength() {

        return Optional.empty();
    }

    @Override
    public NullByteRange doFix(final long length) {

        assert (length > 0);

        if (length <= 0) {

            final String msg = "length must be greater than zero (length=" + length + ")";
            throw new IllegalArgumentException(msg);
        }

        return new NullByteRange();
    }

    @Override
    public NullByteRange doAdjust() {

        return new NullByteRange();
    }

    @Override
    public OpenByteRange doAdjust(final long start) {

        return new OpenByteRange(start);
    }

    @Override
    public ClosedByteRange doAdjust(final long start, final long end) {

        return new ClosedByteRange(start, end);
    }

    @Override
    public boolean equals(final Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {

        return 0;
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .toString();
    }

}
