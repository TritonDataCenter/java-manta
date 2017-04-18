/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.mockito.Mockito;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;
import com.joyent.manta.http.MantaHttpHeaders;

/**
 * Tests for {@code MantaHttpClosedByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
public class MantaHttpClosedByteRangeTest {

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullRange() {

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> range = new MantaHttpClosedByteRange<>(null);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNegativeBounds() {

        final ClosedByteRange range = new ClosedByteRange(Long.MIN_VALUE, -1);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpClosedByteRange<>(range);
    }

    @Test
    private void canAddToHeaders() {

        final String units = MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT;

        final ClosedByteRange range = new ClosedByteRange(1, 3);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpClosedByteRange<>(range);
        final MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        httpRange.addTo(mockHttpHeaders);

        Mockito.verify(mockHttpHeaders).setRange(units + "=1-2");
    }

    @Test
    private void canAddByteToHeaders() {

        final String units = MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT;

        final ClosedByteRange range = new ClosedByteRange(0, 1);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpClosedByteRange<>(range);
        final MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        httpRange.addTo(mockHttpHeaders);

        Mockito.verify(mockHttpHeaders).setRange(units + "=0-0");
    }

    @Test
    private void canAdjust() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final NullByteRange nullRange = new NullByteRange();

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpClosedByteRange<>(range);
        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpNullByteRange = new MantaHttpNullByteRange<>(nullRange);

        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust();

        Assert.assertEquals(adjByteRange, httpNullByteRange);
    }

    @Test
    private void canAdjustOpen() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final OpenByteRange openRange = new OpenByteRange(1);

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpClosedByteRange<>(range);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpOpenByteRange = new MantaHttpOpenByteRange<>(openRange);

        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpOpenByteRange.getStart());

        Assert.assertEquals(adjByteRange, httpOpenByteRange);
    }

    @Test
    private void canAdjustClosed() {

        final ClosedByteRange range = new ClosedByteRange(1, Long.MAX_VALUE - 1);
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE);

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpClosedByteRange<>(range);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpClosedByteRange = new MantaHttpClosedByteRange<>(closedRange);

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpClosedByteRange.getStart(), httpClosedByteRange.getEnd());

        Assert.assertEquals(adjByteRange, httpClosedByteRange);
    }

}
