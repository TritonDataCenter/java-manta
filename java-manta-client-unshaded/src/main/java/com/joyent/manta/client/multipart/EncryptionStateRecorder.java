/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.exception.MantaMemoizationException;
import com.joyent.manta.exception.MantaReflectionException;
import com.joyent.manta.util.CipherCloner;
import com.joyent.manta.util.Cloner;
import com.joyent.manta.util.HmacCloner;
import com.joyent.manta.util.HmacOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.bouncycastle.crypto.macs.HMac;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.UUID;
import javax.crypto.Cipher;

import static org.apache.commons.lang3.reflect.FieldUtils.getField;
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
final class EncryptionStateRecorder {

    /**
     * Private constructor for class only containing static methods.
     */
    private EncryptionStateRecorder() {
    }

    /**
     * Reference to {@link EncryptionContext}'s {@link Cipher} field.
     */
    private static final Field FIELD_ENCRYPTIONCONTEXT_CIPHER =
            getField(EncryptionContext.class, "cipher", true);

    /**
     * Reference to {@link EncryptionContext}'s {@link Cipher} field.
     */
    private static final Field FIELD_ENCRYPTIONSTATE_CIPHERSTREAM =
            getField(EncryptionState.class, "cipherStream", true);

    /**
     * Reference to {@link EncryptionState}'s {@code lastPartAuthWritten} field.
     */
    private static final Field FIELD_ENCRYPTIONSTATE_LASTPARTAUTHWRITTEN =
            getField(EncryptionState.class, "lastPartAuthWritten", true);

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
    static EncryptionStateSnapshot record(final EncryptionState encryptionState, final UUID uploadId) {
        final HMac hmac;

        if (!encryptionState.getEncryptionContext().getCipherDetails().isAEADCipher()) {
            OutputStream cipherStream = encryptionState.getCipherStream();
            final HmacOutputStream digestStream = ensureHmacWrapsCipherStream(cipherStream);
            hmac = CLONER_HMAC.createClone(digestStream.getHmac());
        } else {
            hmac = null;
        }

        final Cipher cipher = CLONER_CIPHER.createClone(encryptionState.getEncryptionContext().getCipher());

        final int bufferSize = encryptionState.getEncryptionContext().getCipherDetails().getBlockSizeInBytes();

        /*
            NOTE: While the MultipartOutputStream buffer is generally only written to _during_ finalization and is
            usually empty during calls to record(), there's no guarantee this buffer will never need to be copied.
            If any changes are made to MultipartOutputStream which result in data residing in that buffer between
            calls to uploadPart this method would continue to work as expected. Additionally the relatively small size
            of these buffers means the performance impact should be negligible.
         */
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(bufferSize);
        try {
            IOUtils.copy(new ByteArrayInputStream(encryptionState.getMultipartStream().getBuf().toByteArray()), buffer);
        } catch (IOException e) {
            throw new MantaMemoizationException("Failed to back up buffer while memoizing encryption state", e);
        }
        final MultipartOutputStream multipartStream = new MultipartOutputStream(bufferSize, buffer);

        final OutputStream cipherStream = EncryptingEntityHelper.makeCipherOutputForStream(
                multipartStream,
                encryptionState.getEncryptionContext().getCipherDetails(),
                cipher,
                hmac);

        return new EncryptionStateSnapshot(
                uploadId,
                encryptionState.getLastPartNumber(),
                encryptionState.isLastPartAuthWritten(),
                cipher,
                cipherStream,
                multipartStream);
    }

    /**
     * Restore Cipher (and potentially HMAC) instances to a provided snapshot. We want to avoid
     * producing a new EncryptionState for a few reasons:
     *
     * 1. The {@code ReentrantLock} in {@code EncryptionState} is used to synchronize access
     * to internal encryption state, including the creation and restoration of snapshots (record and rewind).
     *
     * 2. Interaction between CipherOutputStream and HmacOutputStream
     * makes it non-trivial construct a copy of an EncryptionState that is completely separate from the original.
     */
    static void rewind(final EncryptionState encryptionState, final EncryptionStateSnapshot snapshot) {
        Validate.isTrue(encryptionState.getLastPartNumber() == snapshot.getLastPartNumber(),
                "Snapshot part number must equal encryption state part number");

        try {
            writeField(FIELD_ENCRYPTIONCONTEXT_CIPHER, encryptionState.getEncryptionContext(), snapshot.getCipher());
            writeField(FIELD_ENCRYPTIONSTATE_CIPHERSTREAM, encryptionState, snapshot.getCipherStream());
            writeField(FIELD_ENCRYPTIONSTATE_LASTPARTAUTHWRITTEN, encryptionState, snapshot.getLastPartAuthWritten());
        } catch (IllegalAccessException e) {
            final String message = String.format("Failed to overwrite cipher while rewinding "
                            + "encryption state for upload [%s] part [%s]",
                    snapshot.getUploadId(),
                    snapshot.getLastPartNumber());
            throw new MantaReflectionException(message, e);
        }

        encryptionState.setMultipartStream(snapshot.getMultipartStream());
    }
}
