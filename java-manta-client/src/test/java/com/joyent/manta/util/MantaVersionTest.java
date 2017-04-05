/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class MantaVersionTest {
    public void canLoadVersion() {
        Assert.assertNotNull(MantaVersion.VERSION);
    }

    public void canLoadVersionDate() {
        Assert.assertNotNull(MantaVersion.DATE);
    }
}
