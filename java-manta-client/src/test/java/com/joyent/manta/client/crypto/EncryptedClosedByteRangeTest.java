/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.mockito.AdditionalAnswers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;

/**
 * Tests for {@code EncryptedClosedByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class EncryptedClosedByteRangeTest {

    final int BLOCK_SIZE = 16;
    final int MAC_SIZE = 16;

    final long MAX_PLAINTEXT_SIZE = Long.MAX_VALUE / 2;

    @Mock
    private CipherMap map;

    @Mock
    private SupportedCipherDetails details;
    

    @BeforeClass
    private void init() {

        MockitoAnnotations.initMocks(this);

        Mockito.when(map.plainToCipherStart(Mockito.anyLong())).then(AdditionalAnswers.returnsFirstArg());
        Mockito.when(map.plainToCipherEnd(Mockito.anyLong())).then(AdditionalAnswers.returnsFirstArg());
        Mockito.when(map.plainToCipherOffset(Mockito.anyLong())).thenReturn(Long.valueOf(0));

        Mockito.when(details.getBlockSizeInBytes()).thenReturn(BLOCK_SIZE);
        Mockito.when(details.getAuthenticationTagOrHmacLengthInBytes()).thenReturn(MAC_SIZE);
        Mockito.when(details.getMaximumPlaintextSizeInBytes()).thenReturn(MAX_PLAINTEXT_SIZE);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullRange() {

        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(null, map, details);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullCipherMap() {

        final ClosedByteRange range = new ClosedByteRange(0, 1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, null, details);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithNullCipherDetails() {

        final ClosedByteRange range = new ClosedByteRange(0, 1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, map, null);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithLargeStart() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(mts, mts);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, map, details);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithSmallStart() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(-(mts + 1), -1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedClosedByteRange = new EncryptedClosedByteRange<>(range, map, details);

    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithLargeEnd() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(0, mts + 1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, map, details);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = IllegalArgumentException.class)
    private void willThrowIllegalArgumentExceptionWithSmallEnd() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(-mts, -mts);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedClosedByteRange = new EncryptedClosedByteRange<>(range, map, details);
    }

    @Test
    private void canGetLengthFromPositive() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(0, mts);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, map, details);

        Assert.assertTrue(encryptedRange.getLength().isPresent());
        Assert.assertEquals(encryptedRange.getLength().get(), (Long) mts);
    }

    @Test
    private void canGetLengthFromNegative() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(-mts, -1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedClosedByteRange<>(range, map, details);

        Assert.assertTrue(encryptedRange.getLength().isPresent());
        Assert.assertEquals(encryptedRange.getLength().get(), (Long) (mts - 1));
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = UnsupportedOperationException.class)
    private void canAdjust() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(0, mts);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedClosedByteRange<>(range, map, details);
        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust();
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = UnsupportedOperationException.class)
    private void canAdjustOpen() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(0, mts);
        final OpenByteRange openRange = new OpenByteRange(1);

        final EncryptedOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedOpenByteRange = new EncryptedOpenByteRange<>(openRange, map, details);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedClosedByteRange<>(range, map, details);

        final EncryptedOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust(encryptedOpenByteRange.getStart());
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = UnsupportedOperationException.class)
    private void canAdjustClosed() {

        final long mts = details.getMaximumPlaintextSizeInBytes();

        final ClosedByteRange range = new ClosedByteRange(0, mts);
        final ClosedByteRange closedRange = new ClosedByteRange(0, mts - 1);

        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedClosedByteRange<>(range, map, details);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedClosedByteRange = new EncryptedClosedByteRange<>(closedRange, map, details);

        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust(encryptedClosedByteRange.getStart(), encryptedClosedByteRange.getEnd());
    }

}
