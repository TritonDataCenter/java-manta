/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaEncryptionException;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class EncryptionTypeTest {
    public void canValidateSupportedEncryptionType() {
        String supported = "client/1";
        EncryptionType.validateEncryptionTypeIsSupported(supported);
    }

    public void canCompareEncryptionTypes() {
        Assert.assertEquals(EncryptionType.CLIENT, EncryptionType.CLIENT);
        Assert.assertFalse(EncryptionType.CLIENT.equals(new Object()));
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void nullEncryptionTypeWontValidate() {
        EncryptionType.validateEncryptionTypeIsSupported(null);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void emptyEncryptionTypeWontValidate() {
        EncryptionType.validateEncryptionTypeIsSupported("");
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void blankEncryptionTypeWontValidate() {
        EncryptionType.validateEncryptionTypeIsSupported("   ");
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void unknownTypeEncryptionTypeWontValidate() {
        String unsupported = "foo/2";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void noSeparatorEncryptionTypeWontValidate() {
        String unsupported = "foo2";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void containsExtraSeparatorEncryptionTypeWontValidate() {
        String unsupported = "client/1/";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void versionNotAnIntegerEncryptionTypeWontValidate() {
        String unsupported = "client/bar";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void versionNegativeIntegerEncryptionTypeWontValidate() {
        String unsupported = "client/-1";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void versionTooHighEncryptionTypeWontValidate() {
        String unsupported = "client/999999";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }

    @Test(expectedExceptions = MantaEncryptionException.class)
    public void versionTooLowEncryptionTypeWontValidate() {
        String unsupported = "client/0";
        EncryptionType.validateEncryptionTypeIsSupported(unsupported);
    }
}
