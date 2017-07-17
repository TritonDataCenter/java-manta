/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.exception.MantaReflectionException;
import com.joyent.manta.util.CipherCloner;
import com.joyent.manta.util.Cloner;
import com.joyent.manta.util.HMacCloner;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.io.OutputStream;
import java.lang.reflect.Field;

class EncryptionStateRewinder {

    private final EncryptionState encryptionState;

    private final boolean usesHmac;

    /**
     * Stashed HMac state we can restore to.
     */
    private HMac savedHmac = null;

    /**
     * Stashed Cipher state we can restore to.
     */
    private Cipher savedCipher = null;

    private static Field FIELD_HMACOUTPUTSTREAM_HMAC = FieldUtils.getField(HmacOutputStream.class, "hmac");
    private static Field FIELD_ENCRYPTIONCONTEXT_CIPHER = FieldUtils.getField(EncryptionContext.class, "cipher");

    private Cloner<HMac> hmacCloner = new HMacCloner();
    private Cloner<Cipher> cipherCloner = new CipherCloner();

    public EncryptionStateRewinder(final EncryptionState encryptionState) {
        this.encryptionState = encryptionState;
        this.usesHmac = false == encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher();
    }

    private void ensureDigestWrapsCipherStream(OutputStream cipherStream) {
        if (!cipherStream.getClass().equals(HmacOutputStream.class)) {
            final String message = "Cipher lacks authentication but OutputStream is not HmacOutputStream";
            throw new IllegalStateException(message);
        }
    }

    public void record() {
        if (usesHmac) {
            OutputStream cipherStream = encryptionState.getCipherStream();
            ensureDigestWrapsCipherStream(cipherStream);
            final HmacOutputStream digestStream = (HmacOutputStream) cipherStream;
            savedHmac = hmacCloner.createClone(digestStream.getHmac());
        }

        savedCipher = cipherCloner.createClone(encryptionState.getEncryptionContext().getCipher());
    }

    /**
     *
     */
    public void rewind() {
        // TODO: do we care about repeated calls to rewind without calling record?

        if (usesHmac) {
            if (savedHmac == null) {
                throw new IllegalStateException("rewind called before record, No saved HMac available");
            }

            OutputStream cipherStream = encryptionState.getCipherStream();
            ensureDigestWrapsCipherStream(cipherStream);
            final HmacOutputStream digestStream = ((HmacOutputStream) cipherStream);

            try {
                FieldUtils.writeField(FIELD_HMACOUTPUTSTREAM_HMAC, digestStream, savedHmac, true);
            } catch (IllegalAccessException e) {
                throw new MantaReflectionException("Failed to rewind HmacOutputStream's hmac", e);
            }
        }

        if (savedCipher == null) {
            throw new IllegalStateException("rewind called before record, No saved Cipher available");
        }

        try {
            final EncryptionContext encryptionContext = encryptionState.getEncryptionContext();
            FieldUtils.writeField(FIELD_ENCRYPTIONCONTEXT_CIPHER, encryptionContext, savedCipher, true);
        } catch (IllegalAccessException e) {
            throw new MantaReflectionException("Failed to rewind EncryptionContext's cipher", e);
        }
    }
}
