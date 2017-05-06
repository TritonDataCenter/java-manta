/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.joyent.manta.client.AdjustableRange;
import com.joyent.manta.client.ClosedRange;
import com.joyent.manta.client.FixableRange;
import com.joyent.manta.client.NullRange;
import com.joyent.manta.client.OpenRange;

/**
 * Class supporting encrypted byte range decorator (closed).
 *
 * @param <T> type of a decorated {@code NullRange}
 * @param <U> type of a decorated {@code OpenRange}
 * @param <V> type of decorated {@code ClosedRange}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class EncryptedClosedByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                      U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                      V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>>
       implements ClosedRange,
                  EncryptedByteRange<T, U, V, V>,
                  FixableRange<EncryptedClosedByteRange<T, U, V>> {

    /**
     * Range to translate from plaintext to ciphertext.
     */
    private final V range;

    /**
     * Cipher position map.
     */
    private final CipherMap map;

    /**
     * Cipher details.
     */
    private final SupportedCipherDetails details;

    /**
     * {@code EncryptedClosedByteRange} constructor.
     *
     * @param range range to decorate
     * @param map cipher position map
     * @param details cipher details
     *
     * @throws IllegalArgumentException if {@code range}, {@code map}, or {@code details} are invalid
     */
    public EncryptedClosedByteRange(final V range, final CipherMap map, final SupportedCipherDetails details) {

        validate(range, map, details);

        this.range = range;
        this.map = map;
        this.details = details;
    }

    @Override
    public long getStart() {

        final long s = map.plainToCipherStart(range.getStart());
        final int mcb = details.getAuthenticationTagOrHmacLengthInBytes();


        if (s < 0) {

            return s - mcb;

        } else {

            return s;
        }

    }

    @Override
    public long getEnd() {

        final long e = map.plainToCipherEnd(range.getEnd());
        final int mcb = details.getAuthenticationTagOrHmacLengthInBytes();

        if (e < 0) {

            return e - mcb;

        } else {

            return e;
        }

    }

    @Override
    public Optional<Long> getLength() {

        final long s = getStart();
        final long e = getEnd();

        return Optional.of(e - s);
    }

    @Override
    public long getOffset() {

        final long s = range.getStart();

        return map.plainToCipherOffset(s);
    }

    @Override
    public boolean includesMac() {

        return false;
    }

    @Override
    public EncryptedClosedByteRange<T, U, V> doFix(final long length) {

        final long start = getStart();
        final long end = getEnd();

        assert (length > 0);
        assert (start < length);

        if (length <= 0) {

            final String msg = "length must be greater than zero (length=" + length + ")";
            throw new IllegalArgumentException(msg);
        }

        if (start >= length) {

            final String msg = "length must be greater than start (length=" + length + ", start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

        if (end > length) {

            throw new UnsupportedOperationException("encrypted range does not support fix");

        } else {

            V fixed = range.doAdjust(range.getStart(), range.getEnd());

            return new EncryptedClosedByteRange<T, U, V>(fixed, map, details);
        }

    }

    @Override
    public EncryptedNullByteRange<T, U, V> doAdjust() {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public EncryptedOpenByteRange<T, U, V> doAdjust(final long start) {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public EncryptedClosedByteRange<T, U, V> doAdjust(final long start, final long end) {

        final long s = getStart();
        final long e = getEnd();

        if ((s != start) || (e != end)) {

            throw new UnsupportedOperationException("encrypted range does not support adjust");

        } else {

            V adjusted = range.doAdjust(range.getStart(), range.getEnd());

            return new EncryptedClosedByteRange<T, U, V>(adjusted, map, details);
        }
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

        EncryptedClosedByteRange<?, ?, ?> ecb = (EncryptedClosedByteRange<?, ?, ?>) o;

        return range.equals(ecb.range) && map.equals(ecb.map) && details.equals(ecb.details);
    }

    @Override
    public int hashCode() {

        return Objects.hash(range, map, details);
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .append("range", range)
                .append("map", map)
                .append("details", details)
                .toString();
    }

    /**
     * Method to validate a range, map, and cipher details.
     *
     * @param range the range
     * @param map the cipher position map
     * @param details the cipher details
     *
     *
     * @throws IllegalArgumentException if {@code range}, {@code map}, or {@code
     * details} are invalid
     */
    private static void validate(final ClosedRange range, final CipherMap map,
            final SupportedCipherDetails details) {

        assert (range != null);
        assert (map != null);
        assert (details != null);

        assert ((range.getStart() < details.getMaximumPlaintextSizeInBytes())
                    && (range.getStart() >= -details.getMaximumPlaintextSizeInBytes()));
        assert ((range.getEnd() <= details.getMaximumPlaintextSizeInBytes())
                    && (range.getEnd() > (-details.getMaximumPlaintextSizeInBytes() + 1)));

        if (range == null) {

            throw new IllegalArgumentException("range must not be null");
        }

        if (map == null) {

            throw new IllegalArgumentException("map must not be null");
        }

        if (details == null) {

            throw new IllegalArgumentException("details must not be null");
        }

        final long start = range.getStart();
        final long end = range.getEnd();
        final long maxBytes  = details.getMaximumPlaintextSizeInBytes();

        if ((start >= maxBytes) || (start < -maxBytes)) {

            final String msg = "range start must be within max cipher bytes ["
                    + -maxBytes + ", "
                    + maxBytes + ") (start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

        if ((end > maxBytes) || (end <= -maxBytes)) {

            final String msg = "range end must be within max cipher bytes ("
                    + -maxBytes + ", "
                    + maxBytes + "] (start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

    }

}
