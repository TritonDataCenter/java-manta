/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

public abstract class EncryptionAwareIT {

    private Boolean usingEncryption;
    private String encryptionCipher;

    public Boolean isUsingEncryption() {
        return usingEncryption;
    }

    public String getEncryptionCipher() {
        return encryptionCipher;
    }

    public void setEncryptionParameters(final Boolean usingEncryption, final String encryptionCipher) {
        this.usingEncryption = usingEncryption;
        this.encryptionCipher = encryptionCipher;
    }
}
