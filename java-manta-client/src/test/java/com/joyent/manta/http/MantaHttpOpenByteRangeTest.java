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
 * Tests for {@code MantaHttpOpenByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class MantaHttpOpenByteRangeTest {

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullRange() {

        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> range = new MantaHttpOpenByteRange<>(null);
    }

    @Test
    private void canAdjust() {

        final OpenByteRange range = new OpenByteRange(1);
        final NullByteRange nullRange = new NullByteRange();

        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpOpenByteRange<>(range);
        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpNullByteRange = new MantaHttpNullByteRange<>(nullRange);

        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust();

        Assert.assertEquals(adjByteRange, httpNullByteRange);
    }

    @Test
    private void canAdjustOpen() {

        final OpenByteRange range = new OpenByteRange(1);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpOpenByteRange<>(range);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpByteRange.getStart());

        Assert.assertEquals(adjByteRange, httpByteRange);
    }

    @Test
    private void canAdjustClosed() {

        final OpenByteRange range = new OpenByteRange(1);
        final ClosedByteRange closedRange = new ClosedByteRange(1, Long.MAX_VALUE);

        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpByteRange = new MantaHttpOpenByteRange<>(range);
        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpClosedByteRange = new MantaHttpClosedByteRange<>(closedRange);

        final MantaHttpClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = httpByteRange.doAdjust(httpClosedByteRange.getStart(), httpClosedByteRange.getEnd());

        Assert.assertEquals(adjByteRange, httpClosedByteRange);
    }

    @Test
    private void canAddToHeaders() {

        final String units = MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT;

        final OpenByteRange range = new OpenByteRange(1);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpOpenByteRange<>(range);
        final MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        httpRange.addTo(mockHttpHeaders);

        Mockito.verify(mockHttpHeaders).setRange(units + "=1-");
    }
    
    @Test
    private void canAddOffsetToHeaders() {

        final String units = MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT;

        final OpenByteRange range = new OpenByteRange(-1);
        final MantaHttpOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpOpenByteRange<>(range);
        final MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        httpRange.addTo(mockHttpHeaders);

        Mockito.verify(mockHttpHeaders).setRange(units + "=-1");
    }
}
