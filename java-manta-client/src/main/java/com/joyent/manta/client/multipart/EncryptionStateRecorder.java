/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
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
import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.jcajce.io.CipherOutputStream;

import javax.crypto.Cipher;
import java.io.OutputStream;
import java.lang.reflect.Field;

import static org.apache.commons.lang3.reflect.FieldUtils.getField;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;

/**
 * Helper class for recording state of encryption and authentication state during in-progress MPU uploads.
 * Used to enable retry of {@link EncryptedMultipartManager#uploadPart} in case of network failure.
 * Like the <a href="https://sourcemaking.com/design_patterns/memento">Memento Pattern</a> but with less respect
 * for encapsulation.
 *
 * This class and its methods are package-private because they rely on locking provided by EncryptionState and need
 * to be called a specific points during the MPU process. This class holds none of its own state as as a result
 * all methods have been marked as {@code static}
 */
class EncryptionStateRecorder {

    /**
     * Reference to {@link HmacOutputStream}'s {@link HMac} field.
     */
    private static final Field FIELD_HMACOUTPUTSTREAM_HMAC = getField(HmacOutputStream.class, "hmac", true);

    /**
     * Reference to {@link HmacOutputStream}'s {@link OutputStream} field.
     */
    private static final Field FIELD_HMACOUTPUTSTREAM_OUT = getField(HmacOutputStream.class, "out", true);

    /**
     * Reference to {@link EncryptionContext}'s {@link Cipher} field.
     */
    private static final Field FIELD_ENCRYPTIONCONTEXT_CIPHER = getField(EncryptionContext.class, "cipher", true);

    /**
     * Reference to {@link CipherOutputStream}'s {@link Cipher} field.
     */
    private static final Field FIELD_CIPHEROUTPUTSTREAM_CIPHER = getField(CipherOutputStream.class, "cipher", true);

    /**
     * {@link HMac} cloning helper object.
     */
    private static final Cloner<HMac> CLONER_HMAC = new HmacCloner();

    /**
     * {@link Cipher} cloning helper object.
     */
    private static final Cloner<Cipher> CLONER_CIPHER = new CipherCloner();

    /**
     * Make sure the wrapping stream performs an HMAC digest and cast to the needed type.
     *
     * @param cipherStream the encrypting stream which we are verifying is wrapped in an HMac digest
     * @return the HmacOutputStream
     */
    private static HmacOutputStream ensureHmacWrapsCipherStream(final OutputStream cipherStream) {
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
    static EncryptionStateSnapshot record(final EncryptionState encryptionState) {
        final HMac hmac;

        if (!encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher()) {
            OutputStream cipherStream = encryptionState.getCipherStream();
            final HmacOutputStream digestStream = ensureHmacWrapsCipherStream(cipherStream);
            hmac = CLONER_HMAC.createClone(digestStream.getHmac());
        } else {
            hmac = null;
        }

        final Cipher cipher =
                CLONER_CIPHER.createClone(encryptionState.getEncryptionContext().getCipher());

        return new EncryptionStateSnapshot(encryptionState.getLastPartNumber(), cipher, hmac);
    }

    /**
     * Restore Cipher (and potentially HMAC) instances to a provided snapshot. We want to avoid
     * producing a new EncryptionState for a few reasons:
     *
     * 1. The {@code ReentrantLock} in {@code EncryptionState} is used to synchronize access
     *    to internal encryption state, including the creation and restoration of snapshots (record and rewind).
     * 2. Interaction between CipherOutputStream and HmacOutputStream
     *    makes it non-trivial construct a copy of an EncryptionState that is completely separate from the original.
     */
    static void rewind(final EncryptionState encryptionState, final EncryptionStateSnapshot snapshot) {
        Validate.notNull(snapshot.getCipher(),
                "Snapshot cipher must not be null");
        Validate.isTrue(encryptionState.getLastPartNumber() == snapshot.getLastPartNumber(),
                "Snapshot part number must equal encryption state part number");
        final boolean usesHmac = !encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher();

        final CipherOutputStream cipherStream;
        if (usesHmac) {
            Validate.notNull(snapshot.getHmac(), "Snapshot hmac must not be null");

            final HmacOutputStream digestStream = ensureHmacWrapsCipherStream(encryptionState.getCipherStream());

            try {
                writeField(FIELD_HMACOUTPUTSTREAM_HMAC, digestStream, snapshot.getHmac());
            } catch (IllegalAccessException e) {
                throw new MantaReflectionException("Failed to overwrite HmacOutputStream's hmac", e);
            }

            Object wrappedCipherStream;
            try {
                wrappedCipherStream = readField(FIELD_HMACOUTPUTSTREAM_OUT, digestStream);
            } catch (IllegalAccessException e) {
                throw new MantaReflectionException("Failed to extract wrapped OutputStream", e);
            }

            if (!(wrappedCipherStream instanceof CipherOutputStream)) {
                throw new MantaReflectionException("Expected HmacOutputStream to wrap CipherOutputStream, found: "
                        + wrappedCipherStream.getClass().getCanonicalName());
            }

            cipherStream = (CipherOutputStream) wrappedCipherStream;
        } else {
            cipherStream = (CipherOutputStream) encryptionState.getCipherStream();
        }

        try {
            writeField(FIELD_ENCRYPTIONCONTEXT_CIPHER, encryptionState.getEncryptionContext(), snapshot.getCipher());
            writeField(FIELD_CIPHEROUTPUTSTREAM_CIPHER, cipherStream, snapshot.getCipher());
        } catch (IllegalAccessException e) {
            throw new MantaReflectionException("Failed to overwrite EncryptionContext's cipher", e);
        }
    }
}
