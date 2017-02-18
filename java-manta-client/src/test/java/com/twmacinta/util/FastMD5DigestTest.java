/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.twmacinta.util;

import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class FastMD5DigestTest {
    public void canSaveAndLoadEncodedState() {
        final MD5State initialState = new MD5State();
        initialState.buffer = new byte[] { (byte)23, (byte)34, (byte)56 };
        initialState.state = new int[] { 23423, -5, -33, 232323, Integer.MAX_VALUE };
        initialState.count = 20123;

        final byte[] encodedState = FastMD5Digest.generateEncodedState(initialState);

        final MD5State decodedState = new MD5State();
        FastMD5Digest.updateStateFromEncodedState(decodedState, encodedState);

        AssertJUnit.assertArrayEquals("MD5State buffer arrays do not match",
                initialState.buffer, decodedState.buffer);
        AssertJUnit.assertArrayEquals("MD5State state arrays do not match",
                initialState.state, decodedState.state);
        Assert.assertEquals(decodedState.count, initialState.count,
                "MD5State count value was decoded correctly");
    }
}
