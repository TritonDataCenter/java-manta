/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.util.HMacCloner;
import com.joyent.manta.util.HmacOutputStream;
import com.rits.cloning.Cloner;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.io.OutputStream;

public class EncryptionStateRewinder {

    private final EncryptionState encryptionState;
    protected final boolean usesHmac;
    private HMac savedHmac = null;
    protected Cipher cipher = null;

    public EncryptionStateRewinder(final EncryptionState encryptionState) {
        this.encryptionState = encryptionState;
        this.usesHmac = !encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher();
    }

    private void ensureDigestWrapsCipherStream(OutputStream cipherStream) {
        if (!cipherStream.getClass().equals(HmacOutputStream.class)) {
            final String message = "Cipher lacks authentication but OutputStream is not HmacOutputStream";
            throw new IllegalStateException(message);
        }
    }

    public void record() {
        OutputStream cipherStream = encryptionState.getCipherStream();
        if (usesHmac) {
            ensureDigestWrapsCipherStream(cipherStream);

            final HmacOutputStream digestStream = ((HmacOutputStream) cipherStream);
            savedHmac = new HMacCloner().clone(digestStream.getHmac());
        }

        cipher = new Cloner().deepClone(encryptionState.getEncryptionContext().getCipher());
    }

    public void reset() {
        OutputStream cipherStream = encryptionState.getCipherStream();
        if (usesHmac) {
            ensureDigestWrapsCipherStream(cipherStream);

            final HmacOutputStream digestStream = ((HmacOutputStream) cipherStream);
            savedHmac = new HMacCloner().clone(digestStream.getHmac());
        }

        encryptionState.getEncryptionContext().setCipher(cipher);
    }
}
