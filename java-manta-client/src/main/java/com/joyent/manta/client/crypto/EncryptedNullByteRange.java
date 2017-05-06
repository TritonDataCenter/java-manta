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
 * Class supporting encrypted byte range decorator (null).
 *
 * @param <T> type of decorated {@code NullRange}
 * @param <U> type of a decorated {@code OpenRange}
 * @param <V> type of a decorated {@code ClosedRange}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public class EncryptedNullByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                    U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                    V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>>
       implements NullRange,
                  EncryptedByteRange<T, U, V, T>,
                  FixableRange<EncryptedNullByteRange<T, U, V>> {

    /**
     * Range to translate from plaintext to ciphertext.
     */
    private final T range;

    /**
     * Cipher details.
     */
    private final SupportedCipherDetails details;

    /**
     * {@code EncryptedNullByteRange} constructor.
     *
     * @param range range to decorate
     * @param details cipher details
     *
     * @throws IllegalArgumentException if {@code range} is invalid
     */
    public EncryptedNullByteRange(final T range, final SupportedCipherDetails details) {

        validate(range, details);

        this.range = range;
        this.details = details;
    }

    @Override
    public Optional<Long> getLength() {

        return range.getLength();
    }

    @Override
    public long getOffset() {

        return 0;
    }

    @Override
    public boolean includesMac() {

        return true;
    }

    @Override
    public EncryptedNullByteRange<T, U, V> doFix(final long length) {

        assert (length > 0);

        if (length <= 0) {

            final String msg = "length must be greater than zero (length=" + length + ")";
            throw new IllegalArgumentException(msg);
        }

        T fixed = range.doFix(length);

        return new EncryptedNullByteRange<T, U, V>(fixed, details);
    }

    @Override
    public EncryptedNullByteRange<T, U, V> doAdjust() {

        T adjusted = range.doAdjust();

        return new EncryptedNullByteRange<T, U, V>(adjusted, details);
    }

    @Override
    public EncryptedOpenByteRange<T, U, V> doAdjust(final long start) {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public EncryptedClosedByteRange<T, U, V> doAdjust(final long start, final long end) {

        throw new UnsupportedOperationException("encrypted range does not support adjust");
    }

    @Override
    public T doUndecorate() {

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

        EncryptedNullByteRange<?, ?, ?> enb = (EncryptedNullByteRange<?, ?, ?>) o;

        return range.equals(enb.range) && details.equals(enb.details);
    }

    @Override
    public int hashCode() {

        return Objects.hash(range, details);
    }

    @Override
    public String toString() {

        return new ToStringBuilder(this)
                .append("range", range)
                .append("details", details)
                .toString();
    }

    /**
     * Method to validate a range and cipher details.
     *
     * @param range range
     * @param details cipher details
     *
     * @throws IllegalArgumentException if {@code range} is invalid
     */
    private static void validate(final NullRange range, final SupportedCipherDetails details) {

        assert (range != null);
        assert (details != null);

        if (range == null) {

            throw new IllegalArgumentException("range must not be null");
        }

        if (details == null) {

            throw new IllegalArgumentException("details must not be null");
        }

    }

}
