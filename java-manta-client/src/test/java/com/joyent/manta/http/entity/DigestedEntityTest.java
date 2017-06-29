/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import com.twmacinta.util.FastMD5Digest;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Test
public class DigestedEntityTest {

    public void testWriteToProducesReliableDigest() throws Exception {
        String content = RandomStringUtils.randomAlphanumeric(100);
        DigestedEntity entity = new DigestedEntity(
                new ExposedStringEntity(content, StandardCharsets.UTF_8),
                new FastMD5Digest());

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        entity.writeTo(out);
        byte[] initialDigest = entity.getDigest();

        // connection reset, httpclient performs a retry reusing the same entity
        out.reset();
        entity.writeTo(out);
        byte[] retryDigest = entity.getDigest();

        Assert.assertTrue(
                Arrays.equals(initialDigest, retryDigest),
                "Reuse of DigestedEntity produced differing digest for first retry");

        // connection reset again
        out.reset();
        entity.writeTo(out);
        byte[] extraRetryDigest = entity.getDigest();

        Assert.assertTrue(
                Arrays.equals(initialDigest, extraRetryDigest),
                "Reuse of DigestedEntity produced differing digest for second retry");
    }

}
