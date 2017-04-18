/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@code ClosedByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class ClosedByteRangeTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithEndBeforeStartPositive() {
        final ClosedByteRange range = new ClosedByteRange(1, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithEndEqualStartPositive() {
        final ClosedByteRange range = new ClosedByteRange(0, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithEndEqualStartNegative() {
        final ClosedByteRange range = new ClosedByteRange(-1, -1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithEndBeforeStartNegative() {
        final ClosedByteRange range = new ClosedByteRange(-1, -2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithSmallEnd() {
        final ClosedByteRange range = new ClosedByteRange(-Long.MAX_VALUE, -Long.MAX_VALUE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithMixedSign() {
        final ClosedByteRange range = new ClosedByteRange(1, -1);
    }

    @Test
    private void canGetLengthFromPositive() {

        final ClosedByteRange range = new ClosedByteRange(0, Long.MAX_VALUE);
        final Optional<Long> length = range.getLength();

        Assert.assertTrue(length.isPresent());
        Assert.assertEquals(length.get(), (Long) Long.MAX_VALUE);
    }

    @Test
    private void canGetLengthFromNegative() {

        final ClosedByteRange range = new ClosedByteRange(-Long.MAX_VALUE, -1);
        final Optional<Long> length = range.getLength();

        Assert.assertTrue(length.isPresent());
        Assert.assertEquals(length.get(), (Long) (Long.MAX_VALUE - 1));
    }

    @Test
    private void canAdjust() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE);
        final NullByteRange nullRange = new NullByteRange();
        final NullByteRange adjByteRange = range.doAdjust();

        Assert.assertEquals(adjByteRange, nullRange);
    }

    @Test
    private void canAdjustOpen() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE);
        final OpenByteRange openRange = new OpenByteRange(1);
        final OpenByteRange adjByteRange = range.doAdjust(openRange.getStart());

        Assert.assertEquals(adjByteRange, openRange);
    }

    @Test
    private void canAdjustClosed() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE);
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final ClosedByteRange adjByteRange = range.doAdjust(closedRange.getStart(), closedRange.getEnd());

        Assert.assertEquals(adjByteRange, closedRange);
    }

}
