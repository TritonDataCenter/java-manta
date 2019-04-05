/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
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
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test
public class MantaErrorCodeTest {
    public void valueOfCodeCanFindByMatchingCode() {
        final MantaErrorCode expectedLimitError = MantaErrorCode.INVALID_LIMIT_ERROR;
        final MantaErrorCode expectedNoApiError = MantaErrorCode.NO_API_SERVERS_AVAILABLE;
        final MantaErrorCode expectedObjectNotFoundError = MantaErrorCode.OBJECT_NOT_FOUND_ERROR;
        final MantaErrorCode actualLimitError = MantaErrorCode.valueOfCode("InvalidLimit");
        final MantaErrorCode actualNoApiError = MantaErrorCode.valueOfCode("NoApiServersAvailable");
        final MantaErrorCode actualObjectNotFoundError = MantaErrorCode.valueOfCode("ObjectNotFoundError");


        Assert.assertEquals(actualLimitError, expectedLimitError);
        Assert.assertEquals(actualNoApiError, expectedNoApiError);
        Assert.assertEquals(actualObjectNotFoundError, expectedObjectNotFoundError);
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

    public void valueOfCodeCanFindInDeprectaedErrors() {
        final MantaErrorCode expectedSslError = MantaErrorCode.SSL_REQUIRED_ERROR;
        final MantaErrorCode expectedRootDirectoryError = MantaErrorCode.ROOT_DIRECTORY_ERROR;
        final MantaErrorCode actualSslError = MantaErrorCode.valueOfCode("SSLRequired");
        final MantaErrorCode actualRootDirectoryError = MantaErrorCode.valueOfCode("RootDirectory");

        Assert.assertEquals(actualSslError, expectedSslError);
        Assert.assertEquals(actualRootDirectoryError, expectedRootDirectoryError);
    }
}
