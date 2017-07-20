/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.util.Objects;

/**
 * Snapshots hold references to clones of stateful objects used in streaming encryption operations.
 */
class EncryptionStateSnapshot {

    /**
     * Part number at time of snapshot.
     */
    private final int lastPartNumber;

    /**
     * Cloned HMAC state.
     */
    private final HMac hmac;

    /**
     * Cloned Cipher state.
     */
    private final Cipher cipher;

    /**
     * @param cipher the cloned {@code Cipher}
     * @param hmac the cloned {@code Cipher}
     */
    EncryptionStateSnapshot(final int lastPartNumber, final Cipher cipher, final HMac hmac) {
        this.lastPartNumber = lastPartNumber;
        this.cipher = cipher;
        this.hmac = hmac;
    }

    public int getLastPartNumber() {
        return lastPartNumber;
    }

    public Cipher getCipher() {
        return cipher;
    }

    public HMac getHmac() {
        return hmac;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lastPartNumber, cipher, hmac);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptionStateSnapshot that = (EncryptionStateSnapshot) o;
        return lastPartNumber == that.lastPartNumber
                && cipher == that.cipher
                && hmac == that.hmac;
    }
}
