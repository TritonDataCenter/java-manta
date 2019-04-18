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

import static com.joyent.manta.exception.MantaErrorCode.UNKNOWN_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.SSL_REQUIRED_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.NO_CODE_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.INVALID_LIMIT_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.NO_API_SERVERS_AVAILABLE;
import static com.joyent.manta.exception.MantaErrorCode.OBJECT_NOT_FOUND_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.PARENT_NOT_BUCKET_ERROR;
import static com.joyent.manta.exception.MantaErrorCode.ROOT_DIRECTORY_ERROR;

/**
 * Tests the behavior of the {@link MantaErrorCode} enumeration.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test
public class MantaErrorCodeTest {
    public void valueOfCodeCanFindByMatchingCode() {
        final MantaErrorCode actualLimitError = MantaErrorCode.valueOfCode("InvalidLimit");
        final MantaErrorCode actualNoApiError = MantaErrorCode.valueOfCode("NoApiServersAvailable");
        final MantaErrorCode actualObjectNotFoundError = MantaErrorCode.valueOfCode("ObjectNotFoundError");
        final MantaErrorCode actualParentNotBucketError = MantaErrorCode.valueOfCode("ParentNotBucket");

        Assert.assertEquals(actualLimitError, INVALID_LIMIT_ERROR);
        Assert.assertEquals(actualNoApiError, NO_API_SERVERS_AVAILABLE);
        Assert.assertEquals(actualObjectNotFoundError, OBJECT_NOT_FOUND_ERROR);
        Assert.assertEquals(actualParentNotBucketError, PARENT_NOT_BUCKET_ERROR);
    }

    public void valueOfCodeCanFindByNonmatching() {
        final MantaErrorCode actual = MantaErrorCode.valueOfCode("Who knows?");

        Assert.assertEquals(actual, UNKNOWN_ERROR);
    }

    public void valueOfCodeCanFindByNull() {
        final MantaErrorCode actual = MantaErrorCode.valueOfCode(null);

        Assert.assertEquals(actual, NO_CODE_ERROR);
    }

    @SuppressWarnings("deprecation")
    public void valueOfCodeCanFindInDeprectaedErrors() {
        final MantaErrorCode actualSslError = MantaErrorCode.valueOfCode("SSLRequired");
        final MantaErrorCode actualRootDirectoryError = MantaErrorCode.valueOfCode("RootDirectory");

        Assert.assertEquals(actualSslError, SSL_REQUIRED_ERROR);
        Assert.assertEquals(actualRootDirectoryError, ROOT_DIRECTORY_ERROR);
    }
}
