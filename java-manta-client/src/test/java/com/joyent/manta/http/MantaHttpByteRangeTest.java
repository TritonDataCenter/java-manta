/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import java.math.BigDecimal;

import org.apache.http.HttpHeaders;
import org.mockito.Mockito;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.client.ByteRange;
import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;
import com.joyent.manta.client.Range;
import com.joyent.manta.client.RangeConstructor;
import com.joyent.manta.exception.MantaException;

/**
 * Tests for {@code MantaHttpByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class MantaHttpByteRangeTest {

    private final RangeConstructor<ByteRange<? extends Range>> constructor = new RangeConstructor<ByteRange<? extends Range>>() {

        @Override
        public ByteRange<NullByteRange> constructNull() {

            ByteRange<NullByteRange> range = new NullByteRange();

            return range;
        }

        @Override
        public ByteRange<NullByteRange> constructNull(final long length) {

            NullByteRange range = new NullByteRange();

            return range.doFix(length);
        }

        @Override
        public ByteRange<OpenByteRange> constructOpen(final long start) {

            OpenByteRange range = new OpenByteRange(start);

            return range;
        }

        @Override
        public ByteRange<OpenByteRange> constructOpen(final long start, final long length) {

            OpenByteRange range = new OpenByteRange(start);

            return range.doFix(length);
        }

       @Override
       public ByteRange<ClosedByteRange> constructClosed(final long start, final long end) {

            ClosedByteRange range = new ClosedByteRange(start, end);

            return range;
       }

       @Override
       public ByteRange<ClosedByteRange> constructClosed(final long start, final long end, final long length) {

            ClosedByteRange range = new ClosedByteRange(start, end);

            return range.doFix(length);
       }

    };

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowNumberFormatException() {

        final ByteRange<? extends Range> range = MantaHttpByteRange.rangeFromString("abc", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowNumberFormatExceptionOpenRange() {

        final ByteRange<? extends Range> range = MantaHttpByteRange.rangeFromString("abc-", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowNumberFormatExceptionClosedRange() {

        final ByteRange<? extends Range> range = MantaHttpByteRange.rangeFromString("abc-abc", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowNumberFormatExceptionOffset() {

        MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        Mockito.when(mockHttpHeaders.getRange()).thenReturn(MantaHttpByteRange.HTTP_RANGE_BYTES_UNIT + "=-abc");

        final ByteRange<? extends Range> range = MantaHttpByteRange.rangeFromString("-abc", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionMultipleNegative() {

        final ByteRange<? extends Range> range = MantaHttpByteRange.rangeFromString("=-0--0", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionSmallStart() {

        final BigDecimal s = new BigDecimal(Long.MIN_VALUE)
                                    .subtract(new BigDecimal(1));

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s.toString(), constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionLargeStart() {

        final BigDecimal s = new BigDecimal(Long.MAX_VALUE)
                                    .add(new BigDecimal(1));

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s.toString(), constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionLargeEndA() {

        final BigDecimal s = new BigDecimal(Long.MAX_VALUE)
                                    .subtract(new BigDecimal(1));
        final BigDecimal e = new BigDecimal(Long.MAX_VALUE)
                                    .add(new BigDecimal(1));

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e, constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionLargeEndB() {

        final BigDecimal s = new BigDecimal(Long.MAX_VALUE)
                                    .subtract(new BigDecimal(1));
        final BigDecimal e = new BigDecimal(Long.MAX_VALUE);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e, constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionMalformedLengthA() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(1);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/" + "abc", constructor);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = MantaException.class)
    private void willThrowMantaExceptionMalformedLengthB() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(1);
        final Long l = Long.valueOf(1);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/" + l + "/", constructor);
    }

    @Test
    private void canParseNull() {

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(null, constructor);
        final ByteRange<? extends Range> expected = new NullByteRange();

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseEmptyString() {

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString("", constructor);
        final ByteRange<? extends Range> expected = new NullByteRange();

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseSingleByteRange() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(0);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s.toString(), constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseOpenRange() {

        final Long s = Long.valueOf(0);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-", constructor);
        final ByteRange<? extends Range> expected = new NullByteRange();

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseOpenRangeMaxStart() {

        Long s = Long.valueOf(Long.MAX_VALUE - 1);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-", constructor);
        final ByteRange<? extends Range> expected = new OpenByteRange(Long.MAX_VALUE - 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRange() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(128);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e, constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeLength() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(128);
        final Long l = Long.valueOf(129);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/" + l, constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeMaxLength() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(128);
        final Long l = Long.valueOf(Long.MAX_VALUE);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/" + l, constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeNullRangeLength() {

        final Long l = Long.valueOf(128);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString("/" + l, constructor);
        final ByteRange<? extends Range> expected = new NullByteRange();

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeNullRangeNullLength() {

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString("/", constructor);
        final ByteRange<? extends Range> expected = new NullByteRange();

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeNullLength() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(128);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/", constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeUnknownLength() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(128);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e + "/" + "*", constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseClosedRangeMaxEnd() {

        final Long s = Long.valueOf(0);
        final Long e = Long.valueOf(Long.MAX_VALUE - 1);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s + "-" + e, constructor);
        final ByteRange<? extends Range> expected = new ClosedByteRange(s, e + 1);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseOffsetRange() {

        final Long s = Long.valueOf(-128);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s.toString(), constructor);
        final ByteRange<? extends Range> expected = new OpenByteRange(s);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canParseOffsetRangeMinStart() {

        final Long s = Long.valueOf(-Long.MAX_VALUE);

        final ByteRange<? extends Range> actual = MantaHttpByteRange.rangeFromString(s.toString(), constructor);
        final ByteRange<? extends Range> expected = new OpenByteRange(s);

        Assert.assertEquals(actual, expected);
    }

    @Test
    private void canAddToHeaders() {

        final NullByteRange range = new NullByteRange();
        final MantaHttpNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> httpRange = new MantaHttpNullByteRange<>(range);
        final MantaHttpHeaders mockHttpHeaders = Mockito.mock(MantaHttpHeaders.class);

        httpRange.addTo(mockHttpHeaders);

        Mockito.verify(mockHttpHeaders).remove(HttpHeaders.RANGE);
    }

}
