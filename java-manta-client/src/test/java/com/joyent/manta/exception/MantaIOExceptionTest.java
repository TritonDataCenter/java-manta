/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.util.MantaVersion;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MantaIOExceptionTest {
    public void containsMantaVersionContext() {
        MantaIOException e = new MantaIOException();
        String sdkVersion = e.getContextValues("mantaSdkVersion").get(0).toString();
        Assert.assertNotNull(sdkVersion,
                "SDK version data should be in exception context");
        Assert.assertEquals(sdkVersion, MantaVersion.VERSION,
                "SDK version should equal value coded");
    }
}
