/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

@Test
public class FailingInputStreamTest {

    public void testInputStreamFailsAfterExpectedOffset() throws IOException {
        RandomInputStream randomData = new RandomInputStream(100);
        InputStream fin = new FailingInputStream(randomData, 15);

        int bytesRead = fin.read(new byte[10]);
        Assert.assertEquals(10, bytesRead);

        Assert.assertThrows(() -> {
            fin.read(new byte[10]);
        });
    }
}
