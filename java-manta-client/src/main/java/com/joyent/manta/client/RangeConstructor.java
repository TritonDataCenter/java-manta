/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

/**
 * Interface describing methods to construct a {@code Range}.
 *
 * @param <T> type of constructed range
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface RangeConstructor<T extends Range> {

    /**
     * Method to construct a range with no bounds.
     *
     * @return {@code Range} with no bounds
     */
    T constructNull();

    /**
     * Method to construct a range with no bounds (provided length of containing object).
     *
     * @param length length of containing object
     *
     * @return {@code Range} with no bounds
     */
    T constructNull(long length);

    /**
     * Method to construct a range with a start (inclusive) but no end bound.
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     *
     * @return range with a start but no end bound
     */
    T constructOpen(long start);

    /**
     * Method to construct a range with a start (inclusive) but no end bound (provided length of containing object).
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     * @param length length of containing object
     *
     * @return range with a start but no end bound
     */
    T constructOpen(long start, long length);

    /**
     * Method to construct a range with a start (inclusive) and end (exclusive) bound.
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     * @param end end bound (exclusive negative interpreted as end offset)
     *
     * @return {@code Range} with a start and end bound
     */
    T constructClosed(long start, long end);

    /**
     * Method to construct a range with a start (inclusive) and end (exclusive)
     * bound (provided length of containing object).
     *
     * @param start start bound (inclusive negative interpreted as end offset)
     * @param end end bound (exclusive negative interpreted as end offset)
     * @param length length of containing object
     *
     * @return {@code Range} with a start and end bound
     */
    T constructClosed(long start, long end, long length);

}
