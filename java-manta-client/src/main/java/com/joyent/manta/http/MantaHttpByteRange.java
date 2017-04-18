/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import java.util.Optional;

import com.joyent.manta.client.AdjustableRange;
import com.joyent.manta.client.ClosedRange;
import com.joyent.manta.client.DecoratedRange;
import com.joyent.manta.client.FixableRange;
import com.joyent.manta.client.NullRange;
import com.joyent.manta.client.OpenRange;
import com.joyent.manta.client.Range;
import com.joyent.manta.client.RangeConstructor;
import com.joyent.manta.exception.MantaException;

/**
 * Interface describing a manta http object byte range decorator.
 *
 * @param <T> type of a decorated {@code NullRange}
 * @param <U> type of a decorated {@code OpenRange}
 * @param <V> type of a decorated {@code ClosedRange}
 * @param <W> type of decorated {@code Range}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface MantaHttpByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                    U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                    V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>,
                                    W extends Range>
       extends MantaHttpRange,
               FixableRange<MantaHttpByteRange<T, U, V, W>>,
               AdjustableRange<MantaHttpNullByteRange<T, U, V>,
                               MantaHttpOpenByteRange<T, U, V>,
                               MantaHttpClosedByteRange<T, U, V>>,
               DecoratedRange<W> {

    /**
     * Constant for http range units.
     */
    String HTTP_RANGE_BYTES_UNIT = "bytes";

    /**
     * Method to add range to manta http headers.
     *
     * @param headers manta http headers
     */
    void addTo(MantaHttpHeaders headers);

    /**
     * Method to obtain a {@code long} value from an http {@code range} value.
     *
     * @param value value to parse
     * @param range value range [<a href="https://tools.ietf.org/html/rfc7233">RFC 7233</a>]
     *
     * @return {@code long} parsed from {@code value}
     *
     * @throws MantaException if {@code value} ({@code range}) is malformed
     */
    static long longFromRange(final String range, final String value) {

        try {

            return Long.parseLong(value);

        } catch (NumberFormatException nfe) {

            final String msg = "malformed byte range value (range=" + range + ", value=" + value + ")";
            throw new MantaException(msg, nfe);
        }

    }

    /**
     * Method to construct a {@code Range} from an http range request string.
     *
     * @param <T> type of constructed {@code Range}
     *
     * @param range range [<a href="https://tools.ietf.org/html/rfc7233">RFC 7233</a>]
     * @param constructor range constructor
     *
     * @return range represented in range request string
     *
     * @throws MantaException if range is malformed
     * @throws IllegalArgumentException if constructor is null
     *
     * @see com.joyent.manta.client.ByteRange
     * @see com.joyent.manta.client.crypto.EncryptedByteRange
     * @see com.joyent.manta.http.MantaHttpByteRange
     */
    static <T extends Range> T rangeFromString(final String range, final RangeConstructor<T> constructor) {

        assert (constructor != null);

        Optional<Long> length = Optional.empty();

        if (constructor == null) {

            final String msg = "constructor must not be null";
            throw new IllegalArgumentException(msg);
        }

        // better safe
        if (range == null) {

            return constructor.constructNull();
        }

        String buf = range.trim();

        // length
        if (buf.contains("/")) {

            String value = buf.substring(buf.indexOf("/") + 1);

            if (!value.isEmpty()) {

                if (!value.equals("*")) {

                    length = Optional.of(longFromRange(range, value));
                }
            }

            buf = buf.substring(0, buf.indexOf("/"));
        }

        // better safe
        if (buf.isEmpty()) {

            return constructor.constructNull();

        // better safe
        } else if (!buf.contains("-")) {

            final String value = buf;

            final long b = longFromRange(range, value);

            return constructor.constructClosed(b, b + 1);

        } else if (buf.startsWith("-")) {

            final String value = buf;

            final long s = longFromRange(range, value);

            if (length.isPresent()) {

                final long l = length.get();

                return constructor.constructOpen(s, l);

            } else {

                return constructor.constructOpen(s);
            }

        } else if (buf.endsWith("-")) {

            final String value = buf.substring(0, buf.length() - 1);

            final long s = longFromRange(range, value);

            if (s == 0) {

                if (length.isPresent()) {

                    final long l = length.get();

                    return constructor.constructNull(l);

                } else {

                    return constructor.constructNull();
                }

            } else {

                if (length.isPresent()) {

                    final long l = length.get();

                    return constructor.constructOpen(s, l);

                } else {

                    return constructor.constructOpen(s);
                }
            }

        } else {

            final String[] bounds = buf.split("-");

            if (bounds.length > 2) {

                final String msg = "malformed byte range (range=" + range + ")";
                throw new MantaException(msg);
            }

            final String start = bounds[0];
            final String end = bounds[1];

            final long s = longFromRange(range, start);
            final long e = longFromRange(range, end);

            if (e == Long.MAX_VALUE) {

                final String msg = "malformed byte range end (range="
                                        + range + ", end="
                                        + end + ")";
                throw new MantaException(msg);
            }

            if (length.isPresent()) {

                final long l = length.get();

                return constructor.constructClosed(s, e + 1, l);

            } else {

                return constructor.constructClosed(s, e + 1);
            }

        }

    }

}
