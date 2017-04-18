/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;

/**
 * Tests for {@code MantaHttpNullByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class MantaHttpNullByteRangeTest {

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullRange() {

        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> range = new MantaHttpNullByteRange<>(null);
    }

    @Test
    private void canAdjust() {

        final NullByteRange range = new NullByteRange();
        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpNullByteRange<>(range);
        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust();

        Assert.assertEquals(adjByteRange, httpByteRange);
    }

    @Test
    private void canAdjustOpen() {

        final NullByteRange range = new NullByteRange();
        final OpenByteRange openRange = new OpenByteRange(1);

        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpNullByteRange<>(range);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpOpenByteRange = new MantaHttpOpenByteRange<>(openRange);

        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpOpenByteRange.getStart());

        Assert.assertEquals(adjByteRange, httpOpenByteRange);
    }

    @Test
    private void canAdjustClosed() {

        final NullByteRange range = new NullByteRange();
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE - 1);

        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpNullByteRange<>(range);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpClosedByteRange = new MantaHttpClosedByteRange<>(closedRange);

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpClosedByteRange.getStart(), httpClosedByteRange.getEnd());

        Assert.assertEquals(adjByteRange, httpClosedByteRange);
    }

}
