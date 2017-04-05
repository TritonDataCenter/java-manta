/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class ByteRangeConversionTest {
    public void setsCorrectProperties() {
        ByteRangeConversion byteConversion = new ByteRangeConversion(100, 10, 80, 70, 1);
        Assert.assertEquals(byteConversion.getCiphertextStartPositionInclusive(), 100);
        Assert.assertEquals(byteConversion.getPlaintextBytesToSkipInitially(), 10);
        Assert.assertEquals(byteConversion.getCiphertextEndPositionInclusive(), 80);
        Assert.assertEquals(byteConversion.getLengthOfPlaintextIncludingSkipBytes(), 70);
        Assert.assertEquals(byteConversion.getStartingBlockNumberInclusive(), 1);
    }
}
