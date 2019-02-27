/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class RandomInputStreamTest {
    public void canDoSingleByteReadsForOnlyTheExpectedNumberOfBytes() throws IOException {
        final long expectedBytes = 1119;

        try (RandomInputStream stream = new RandomInputStream(expectedBytes)) {
            long actualBytesRead = 0;

            for (int readVal = stream.read(); readVal != -1; readVal = stream.read()) {
                actualBytesRead++;
            }

            Assert.assertEquals(actualBytesRead, expectedBytes,
                    "Number bytes actually read didn't match actual bytes read");
        }
    }

    public void canDoMultiByteReadsForOnlyTheExpectedNumberOfBytes() throws IOException {
        final long expectedBytes = 1119;

        try (RandomInputStream stream = new RandomInputStream(expectedBytes)) {
            long actualBytesRead = 0;
            int lastReadBytes;
            byte[] buffer = new byte[16];

            while ((lastReadBytes = stream.read(buffer, 0, 16)) != -1) {
                actualBytesRead += lastReadBytes;
            }

            Assert.assertEquals(actualBytesRead, expectedBytes,
                    "Number bytes actually read didn't match actual bytes read");
        }
    }
}
