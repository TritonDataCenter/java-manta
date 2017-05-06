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
 * Tests for {@code EncryptedNullByteRange}.
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 */
@Test
public class EncryptedNullByteRangeTest {

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

        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedRange = new EncryptedNullByteRange<>(null, details);
    }

    @Test
    private void canGetLength() {

        final NullByteRange range = new NullByteRange();
        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedNullByteRange<>(range, details);

        Assert.assertFalse(encryptedByteRange.getLength().isPresent());
    }

    @Test
    private void canAdjust() {

        final NullByteRange range = new NullByteRange();
        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedNullByteRange<>(range, details);
        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust();

        Assert.assertEquals(adjByteRange, encryptedByteRange);
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = UnsupportedOperationException.class)
    private void canAdjustOpen() {

        final NullByteRange range = new NullByteRange();
        final OpenByteRange openRange = new OpenByteRange(1);

        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedNullByteRange<>(range, details);
        final EncryptedOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedOpenByteRange = new EncryptedOpenByteRange<>(openRange, map, details);

        final EncryptedOpenByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust(encryptedOpenByteRange.getStart());
    }

    @SuppressWarnings("unused")
    @Test(expectedExceptions = UnsupportedOperationException.class)
    private void canAdjustClosed() {

        final NullByteRange range = new NullByteRange();
        final ClosedByteRange closedRange = new ClosedByteRange(0, details.getMaximumPlaintextSizeInBytes() - 1);

        final EncryptedNullByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedByteRange = new EncryptedNullByteRange<>(range, details);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> encryptedClosedByteRange = new EncryptedClosedByteRange<>(closedRange, map, details);

        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange> adjByteRange = encryptedByteRange.doAdjust(encryptedClosedByteRange.getStart(), encryptedClosedByteRange.getEnd());
    }

}
