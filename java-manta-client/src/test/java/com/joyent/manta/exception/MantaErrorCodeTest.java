/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests the behavior of the {@link MantaErrorCode} enumeration.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test
public class MantaErrorCodeTest {
    public void valueOfCodeCanFindByMatchingCode() {
        final MantaErrorCode expected = MantaErrorCode.INVALID_LIMIT_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode("InvalidLimit");

        Assert.assertEquals(actual, expected);
    }

    public void valueOfCodeCanFindByNonmatching() {
        final MantaErrorCode expected = MantaErrorCode.UNKNOWN_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode("Who knows?");

        Assert.assertEquals(actual, expected);
    }

    public void valueOfCodeCanFindByNull() {
        final MantaErrorCode expected = MantaErrorCode.NO_CODE_ERROR;
        final MantaErrorCode actual = MantaErrorCode.valueOfCode(null);

        Assert.assertEquals(actual, expected);
    }
}
