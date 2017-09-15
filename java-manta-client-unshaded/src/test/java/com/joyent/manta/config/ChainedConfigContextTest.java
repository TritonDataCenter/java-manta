/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test
public class ChainedConfigContextTest {
    @Test(groups = { "config" })
    public void defaultsWontOverwriteContext() {
        final String expectedKeyPath = "/home/dude/.ssh/my_key";
        final String expectedUrl = "https://manta.another.host.com";
        StandardConfigContext context1 = new StandardConfigContext();
        context1.setMantaKeyPath(expectedKeyPath);
        context1.setMantaURL("https://manta.host.com");
        context1.setClientEncryptionEnabled(true);
        context1.setTimeout(3000);
        context1.setTcpSocketTimeout(40);

        StandardConfigContext context2 = new StandardConfigContext();
        context2.setMantaURL(expectedUrl);
        context2.setRetries(2);
        context2.setTcpSocketTimeout(100);
        context2.setHttpBufferSize(1024);

        DefaultsConfigContext defaults = new DefaultsConfigContext();
        ChainedConfigContext chained = new ChainedConfigContext(
                context1, context2, defaults);

        Assert.assertEquals(chained.getHttpsCipherSuites(), defaults.getHttpsCipherSuites());
        Assert.assertEquals(chained.getHttpsProtocols(), defaults.getHttpsProtocols());
        Assert.assertEquals(chained.getEncryptionAuthenticationMode(), defaults.getEncryptionAuthenticationMode());
        Assert.assertTrue(Arrays.equals(chained.getEncryptionPrivateKeyBytes(), defaults.getEncryptionPrivateKeyBytes()));
        Assert.assertEquals(chained.getPrivateKeyContent(), defaults.getPrivateKeyContent());
        Assert.assertEquals(chained.getEncryptionPrivateKeyPath(), defaults.getEncryptionPrivateKeyPath());
        Assert.assertEquals(chained.getMantaHomeDirectory(), defaults.getMantaHomeDirectory());
        Assert.assertEquals(chained.getMantaKeyId(), defaults.getMantaKeyId());

        Assert.assertEquals(chained.getTcpSocketTimeout().intValue(), 100);
        Assert.assertEquals(chained.getHttpBufferSize().intValue(), 1024);

        // This is important because we need to confirm that it isn't overwritten
        Assert.assertEquals(chained.getMantaKeyPath(), expectedKeyPath);
        Assert.assertEquals(chained.getMantaURL(), expectedUrl);
        Assert.assertEquals(chained.getTimeout(), context1.getTimeout());
        Assert.assertEquals(chained.getRetries(), context2.getRetries());
        Assert.assertEquals(chained.getMantaUser(), defaults.getMantaUser());
        Assert.assertEquals(chained.getPassword(), defaults.getPassword());
        Assert.assertEquals(chained.getMaximumConnections(), defaults.getMaximumConnections());
        Assert.assertEquals(chained.isClientEncryptionEnabled(), context1.isClientEncryptionEnabled());
        Assert.assertEquals(chained.disableNativeSignatures(), defaults.disableNativeSignatures());
    }
}
