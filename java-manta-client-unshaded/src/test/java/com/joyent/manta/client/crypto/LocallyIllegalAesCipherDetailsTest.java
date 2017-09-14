/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.testng.annotations.Test;

public class LocallyIllegalAesCipherDetailsTest {

    @Test(expectedExceptions = Error.class,
          expectedExceptionsMessageRegExp = "This cipher is not compatible with the current runtime: .*")
    public void throwsUncheckedErrorsForAnyMethodCall() {
        // keyLengthBits passed here is for error reporting, no validation is applied
        LocallyIllegalAesCipherDetails illegalAesCipherDetails = new LocallyIllegalAesCipherDetails(0);

        illegalAesCipherDetails.getCipher(); // or any other method call from the SupportedCipherDetails interface
    }
}