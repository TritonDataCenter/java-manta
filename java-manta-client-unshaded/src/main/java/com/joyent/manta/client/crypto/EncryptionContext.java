/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.OutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.util.Objects;

/**
 * Context class that contains a secret key, cipher/mode properties objects
 * and a cipher that is initialized in encryption mode. The {@link Cipher}
 * object contains the current state of encryption, so it can be passed
 * between streams to resume encrypting even if it is given different inputs.
 *
 * @author <a href="https://github.com/cburroughs/">Chris Burroughs</a>
 * @since 3.0.0
 */
public class EncryptionContext {
    /**
     * Secret key to encrypt stream with.
     */
    private transient SecretKey key;

    /**
     * Attributes of the cipher used for encryption.
     */
    private final SupportedCipherDetails cipherDetails;

    /**
     * Cipher implementation used to encrypt as a stream.
     */
    private final Cipher cipher;

    /**
     * Flag indicating that we will only use cloneable ciphers for encryption
     * operations.
     */
    private final boolean useCloneableCipher;

    /**
     * Creates a new instance of an encryption context.
     *
     * @param key secret key to initialize cipher with
     * @param cipherDetails cipher/mode properties object to create cipher object from
     * @param useCloneableCipher true when only cloneable ciphers can be used
     */
    public EncryptionContext(final SecretKey key,
                             final SupportedCipherDetails cipherDetails,
                             final boolean useCloneableCipher) {
        this(key, cipherDetails, null, useCloneableCipher);
    }

    /**
     * Creates a new instance of an encryption context using an existing IV value. This
     * is used in {@link EncryptingEntity} to ensure that the state of the underlying
     * {@link Cipher} is unaffected by retries since {@link EncryptingEntity#writeTo(OutputStream)}
     * may be called multiple times.
     *
     * @param key secret key to initialize cipher with
     * @param cipherDetails cipher/mode properties object to create cipher object from
     * @param suppliedIv an existing IV to reuse
     * @param useCloneableCipher true when only cloneable ciphers can be used
     */
    EncryptionContext(final SecretKey key,
                      final SupportedCipherDetails cipherDetails,
                      final byte[] suppliedIv,
                      final boolean useCloneableCipher) {

        @SuppressWarnings("MagicNumber")
        final int keyBits = key.getEncoded().length << 3; // convert bytes to bits

        if (keyBits != cipherDetails.getKeyLengthBits()) {
            String msg = "Mismatch between algorithm definition and secret key size";
            MantaClientEncryptionException e = new MantaClientEncryptionException(msg);
            e.setContextValue("cipherDetails", cipherDetails.toString());
            e.setContextValue("secretKeyAlgorithm", key.getAlgorithm());
            e.setContextValue("secretKeySizeInBits", String.valueOf(keyBits));
            e.setContextValue("expectedKeySizeInBits", cipherDetails.getKeyLengthBits());
            throw e;
        }

        this.key = key;
        this.useCloneableCipher = useCloneableCipher;
        this.cipherDetails = cipherDetails;

        if (useCloneableCipher) {
            this.cipher = cipherDetails.getCloneableCipher();
        } else {
            this.cipher = cipherDetails.getCipher();
        }

        initializeCipher(suppliedIv);
    }

    public SecretKey getSecretKey() {
        return key;
    }

    /**
     * Sets the secret key (used by serialization).
     *
     * @param key secret key
     */
    public void setKey(final SecretKey key) {
        this.key = key;
    }

    public SupportedCipherDetails getCipherDetails() {
        return cipherDetails;
    }

    public Cipher getCipher() {
        return cipher;
    }

    /**
     * @return when true, this encryption context requires a cloneable cipher
     */
    public boolean usesCloneableCipher() {
        return useCloneableCipher;
    }

    /**
     * Initializes the cipher with an IV (initialization vector), so that
     * the cipher is ready to be used to encrypt.
     * @param suppliedIv IV to use in case of a retry or test case, null indicates we should generate one
     */
    private void initializeCipher(final byte[] suppliedIv) {
        try {
            final byte[] iv;
            if (suppliedIv != null) {
                iv = suppliedIv;
            } else {
                iv = cipherDetails.generateIv();
            }
            cipher.init(Cipher.ENCRYPT_MODE, this.key, cipherDetails.getEncryptionParameterSpec(iv));
        } catch (InvalidKeyException e) {
            MantaClientEncryptionException mcee = new MantaClientEncryptionException(
                    "There was a problem loading private key", e);
            String details = String.format("key=%s, algorithm=%s",
                    key.getAlgorithm(), key.getFormat());
            mcee.setContextValue("key_details", details);
            throw mcee;
        } catch (InvalidAlgorithmParameterException e) {
            throw new MantaClientEncryptionException(
                    "There was a problem with the passed algorithm parameters", e);
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final EncryptionContext that = (EncryptionContext) o;

        return Objects.equals(key, that.key)
               && Objects.equals(cipherDetails, that.cipherDetails)
               && useCloneableCipher == that.useCloneableCipher;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, cipherDetails, useCloneableCipher);
    }
}
