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
import com.joyent.manta.util.HmacCloner;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.io.OutputStream;
import java.lang.reflect.Field;

/**
 * Helper class for recording state of encryption and authentication state during in-progress MPU uploads.
 * Used to enable retry of {@link EncryptedMultipartManager#uploadPart} in case of network failure.
 * Like the <a href="https://sourcemaking.com/design_patterns/memento">Memento Pattern</a> but with less respect
 * for encapsulation.
 */
public class EncryptionStateRecorder {

    /**
     * {@link EncryptionState} to make rewindable.
     */
    private final EncryptionState encryptionState;

    /**
     * Whether or not separate authentication is in use.
     * True indicates we need to back up HMAC state.
     * False indicates the Cipher has built-in authentication.
     */
    private final boolean usesHmac;

    /**
     * Stashed HMac state we can restore to.
     */
    private HMac savedHmac = null;

    /**
     * Stashed Cipher state we can restore to.
     */
    private Cipher savedCipher = null;

    /**
     * Reference to {@link HmacOutputStream}'s {@link HMac} field.
     */
    private static final Field FIELD_HMACOUTPUTSTREAM_HMAC =
            FieldUtils.getField(HmacOutputStream.class, "hmac", true);

    /**
     * Reference to {@link EncryptionContext}'s {@link Cipher} field.
     */
    private static final Field FIELD_ENCRYPTIONCONTEXT_CIPHER =
            FieldUtils.getField(EncryptionContext.class, "cipher", true);

    /**
     * {@link HMac} cloning helper object.
     */
    private static final Cloner<HMac> CLONER_HMAC = new HmacCloner();

    /**
     * {@link Cipher} cloning helper object.
     */
    private static final Cloner<Cipher> CLONER_CIPHER = new CipherCloner();

    /**
     * Construct an EncryptionStateRecorder which can be used to rewind the state of the given {@link EncryptionState}.
     *
     * @param encryptionState object to extract state from (and potentially write back to)
     */
    public EncryptionStateRecorder(final EncryptionState encryptionState) {
        this.encryptionState = encryptionState;
        this.usesHmac = !encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher();
    }

    /**
     * Make sure the wrapping stream performs an HMAC digest and cast the needed type.
     *
     * @param cipherStream the encrypting stream which we are verifying is wrapped in an HMac digest
     */
    private HmacOutputStream ensureHmacWrapsCipherStream(final OutputStream cipherStream) {
        if (!cipherStream.getClass().equals(HmacOutputStream.class)) {
            final String message = "Cipher lacks authentication but OutputStream is not HmacOutputStream";
            throw new IllegalStateException(message);
        }

        return (HmacOutputStream) cipherStream;
    }

    /**
     * Clones Cipher (and potentially HMAC) instances for future use.
     * This should be called before data is streamed in uploadPart.
     */
    public void record() {
        if (usesHmac) {
            OutputStream cipherStream = encryptionState.getCipherStream();
            final HmacOutputStream digestStream = ensureHmacWrapsCipherStream(cipherStream);
            savedHmac = CLONER_HMAC.createClone(digestStream.getHmac());
        }

        savedCipher = CLONER_CIPHER.createClone(encryptionState.getEncryptionContext().getCipher());
    }

    /**
     * Restore the saved Cipher (and potentially HMAC) instances.
     */
    public void rewind() {
        // NOTE: do we care about repeated calls to rewind without respective calls record?

        if (usesHmac) {
            if (savedHmac == null) {
                throw new IllegalStateException("rewind called before record, No saved HMac available");
            }

            OutputStream cipherStream = encryptionState.getCipherStream();
            final HmacOutputStream digestStream = ensureHmacWrapsCipherStream(cipherStream);

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
