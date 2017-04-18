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
 * Tests for {@code OpenByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class OpenByteRangeTest {

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithLargeStart() {
        final OpenByteRange range = new OpenByteRange(Long.MAX_VALUE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    @SuppressWarnings("unused")
    private void willThrowIllegalArgumentExceptionWithSmallStart() {
        final OpenByteRange range = new OpenByteRange(Long.MIN_VALUE);
    }

    @Test
    private void canGetLengthFromPositive() {

        final OpenByteRange range = new OpenByteRange(Long.MAX_VALUE - 1);
        final Optional<Long> length = range.getLength();

        Assert.assertFalse(length.isPresent());
    }

    @Test
    private void canGetLengthFromNegative() {

        final OpenByteRange range = new OpenByteRange(Long.MIN_VALUE + 1);
        final Optional<Long> length = range.getLength();

        Assert.assertTrue(length.isPresent());
        Assert.assertEquals(length.get(), (Long) Long.MAX_VALUE);
    }

    @Test
    private void canAdjust() {

        final OpenByteRange range = new OpenByteRange(1);
        final NullByteRange nullRange = new NullByteRange();
        final NullByteRange adjByteRange = range.doAdjust();

        Assert.assertEquals(adjByteRange, nullRange);
    }

    @Test
    private void canAdjustOpen() {

        final OpenByteRange range = new OpenByteRange(1);
        final OpenByteRange openRange = new OpenByteRange(1);
        final OpenByteRange adjByteRange = range.doAdjust(openRange.getStart());

        Assert.assertEquals(adjByteRange, openRange);
    }

    @Test
    private void canAdjustClosed() {

        final OpenByteRange range = new OpenByteRange(1);
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final ClosedByteRange adjByteRange = range.doAdjust(closedRange.getStart(), closedRange.getEnd());

        Assert.assertEquals(adjByteRange, closedRange);
    }

}
