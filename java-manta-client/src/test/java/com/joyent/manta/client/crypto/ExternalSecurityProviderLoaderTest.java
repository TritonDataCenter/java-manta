/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class ExternalSecurityProviderLoaderTest {
    public void canLoadBouncyCastleProvider() {
        Assert.assertNotNull(ExternalSecurityProviderLoader.getBouncyCastleProvider(),
                "Bouncy Castle provider wasn't loaded");
    }
}
