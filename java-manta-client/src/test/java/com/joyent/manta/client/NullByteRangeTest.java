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
 * Tests for {@code NullByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class NullByteRangeTest {

    @Test
    private void canGetLength() {

        final NullByteRange range = new NullByteRange();
        final Optional<Long> length = range.getLength();

        Assert.assertFalse(length.isPresent());
    }

    @Test
    private void canAdjust() {

        final NullByteRange range = new NullByteRange();
        final NullByteRange adjByteRange = range.doAdjust();

        Assert.assertEquals(adjByteRange, range);
    }

    @Test
    private void canAdjustOpen() {

        final NullByteRange range = new NullByteRange();
        final OpenByteRange openRange = new OpenByteRange(1);
        final OpenByteRange adjByteRange = range.doAdjust(openRange.getStart());

        Assert.assertEquals(adjByteRange, openRange);
    }

    @Test
    private void canAdjustClosed() {

        final NullByteRange range = new NullByteRange();
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final ClosedByteRange adjByteRange = range.doAdjust(closedRange.getStart(), closedRange.getEnd());

        Assert.assertEquals(adjByteRange, closedRange);
    }

}
