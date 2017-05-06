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
 * Class supporting encrypted byte range decorator (open).
 *
 * @param <T> type of a decorated {@code NullRange}
 * @param <U> type of decorated {@code OpenRange}
 * @param <V> type of a decorated {@code ClosedRange}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class EncryptedOpenByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                    U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                    V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>>
       implements OpenRange,
                  EncryptedByteRange<T, U, V, U>,
                  FixableRange<EncryptedOpenByteRange<T, U, V>> {

    /**
     * Range to translate from plaintext to ciphertext.
     */
    private final U range;

    /**
     * Cipher position map.
     */
    private final CipherMap map;

    /**
     * Cipher details.
     */
    private final SupportedCipherDetails details;

    /**
     * {@code EncryptedOpenByteRange} constructor.
     *
     * @param range range to decorate
     * @param map cipher position map
     * @param details cipher details
     *
     * @throws IllegalArgumentException if {@code range}, {@code map}, or {@code details} are invalid
     */
    public EncryptedOpenByteRange(final U range, final CipherMap map, final SupportedCipherDetails details) {

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
    public Optional<Long> getLength() {

        final long s = getStart();

        if (s < 0) {

            return Optional.of(-s);

        } else {

            return Optional.empty();
        }
    }

    @Override
    public long getOffset() {

        final long s = range.getStart();

        return map.plainToCipherOffset(s);
    }

    @Override
    public boolean includesMac() {

        return true;
    }

    @Override
    public EncryptedOpenByteRange<T, U, V> doFix(final long length) {

        final long start = getStart();

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

        if (start < -length) {

            throw new UnsupportedOperationException("encrypted range does not support fix");

        } else {

            U fixed = range.doAdjust(range.getStart());

            return new EncryptedOpenByteRange<T, U, V>(fixed, map, details);
        }

    }

    @Override
    public EncryptedNullByteRange<T, U, V> doAdjust() {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public EncryptedOpenByteRange<T, U, V> doAdjust(final long start) {

        final long s = getStart();

        if (s != start) {

            throw new UnsupportedOperationException("encrypted range does not support adjust");

        } else {

            U adjusted = range.doAdjust(range.getStart());

            return new EncryptedOpenByteRange<T, U, V>(adjusted, map, details);
        }

    }

    @Override
    public EncryptedClosedByteRange<T, U, V> doAdjust(final long start, final long end) {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public U doUndecorate() {

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

        EncryptedOpenByteRange<?, ?, ?> eob = (EncryptedOpenByteRange<?, ?, ?>) o;

        return range.equals(eob.range) && map.equals(eob.map) && details.equals(eob.details);
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
     * @param range range
     * @param map cipher position map
     * @param details cipher details
     *
     * @throws IllegalArgumentException if {@code range}, {@code map}, or {@code
     * details} are invalid
     */
    private static void validate(final OpenRange range, final CipherMap map,
            final SupportedCipherDetails details) {

        assert (range != null);
        assert (map != null);
        assert (details != null);

        assert ((range.getStart() < details.getMaximumPlaintextSizeInBytes())
                    && (range.getStart() >= -details.getMaximumPlaintextSizeInBytes()));

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
        final long maxBytes  = details.getMaximumPlaintextSizeInBytes();

        if ((start >= maxBytes) || (start < -maxBytes)) {

            final String msg = "range start must be within max cipher bytes ["
                    + -maxBytes + ", "
                    + maxBytes + ") (start=" + start + ")";
            throw new IllegalArgumentException(msg);
        }

    }

}
