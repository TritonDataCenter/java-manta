/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http.entity;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

@Test
public class ExposedStringEntityTest {
    public void canSetContentLength() {
        final String string = "I am a string";
        ExposedStringEntity entity = new ExposedStringEntity(string, StandardCharsets.US_ASCII);

        Assert.assertEquals(entity.getContentLength(),
                string.length(), "String length should equal content-length");
    }
}
