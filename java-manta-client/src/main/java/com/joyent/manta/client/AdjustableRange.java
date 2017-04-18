/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

/**
 * Interface describing a range (adjustable).
 *
 * @param <T> type of range (null)
 * @param <U> type of range (open)
 * @param <V> type of range (closed)
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface AdjustableRange<T extends NullRange & AdjustableRange<T, U, V>,
                                 U extends OpenRange & AdjustableRange<T, U, V>,
                                 V extends ClosedRange & AdjustableRange<T, U, V>>
       extends Range {

    /**
     * Method to adjust range bounds (null).
     *
     * @return adjusted range
     *
     * @see com.joyent.manta.client.NullRange
     */
    T doAdjust();

    /**
     * Method to adjust range bounds (open).
     *
     * @param start adjusted range start
     *
     * @return adjusted range
     *
     * @see com.joyent.manta.client.OpenRange
     */
    U doAdjust(long start);

    /**
     * Method to adjust range bounds (closed).
     *
     * @param start adjusted range start
     * @param end adjusted range end
     *
     * @return adjusted range
     *
     * @see com.joyent.manta.client.ClosedRange
     */
    V doAdjust(long start, long end);

}
