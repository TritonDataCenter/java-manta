/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.security.NoSuchAlgorithmException;
import java.security.Provider;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Interface describing a cipher that is supported by the Manta SDK.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface SupportedCipherDetails {
    /**
     * @return algorithm name used by key generation (e.g. {@link SecretKeyUtils#generate(String, int)})
     */
    String getKeyGenerationAlgorithm();

    /**
     * @return Unique identifier used for identifying cipher in Manta
     */
    String getCipherId();

    /**
     * @return algorithm name used when initializing a cipher
     */
    String getCipherAlgorithm();

    /**
     * @return number of bits in the key
     */
    int getKeyLengthBits();

    /**
     * @return encryption cipher block size in bytes
     */
    int getBlockSizeInBytes();

    /**
     * @return initialization vector length in bytes
     */
    int getIVLengthInBytes();

    /**
     * @return size of authentication tag or MAC in bytes
     */
    int getAuthenticationTagOrHmacLengthInBytes();

    /**
     * @return maximum size of plaintext data in bytes
     */
    long getMaximumPlaintextSizeInBytes();

    /**
     * @return a new instance of the associated cipher
     */
    Cipher getCipher();

    /**
     * Calculates the size of the output ciphertext based on the plaintext
     * size.
     *
     * @param plaintextSize size of the plaintext input
     * @return size of the ciphertext output
     */
    long ciphertextSize(long plaintextSize);

    /**
     * Calculates the size of the plaintext data based on the ciphertext
     * size. In some cases, this value will be just an estimation.
     *
     * @param ciphertextSize size of the ciphertext input
     * @return size of the plaintext output
     */
    long plaintextSize(long ciphertextSize);

    /**
     * Flag indicating of the plaintext size calculation is not exact.
     * @return true if the value returned from plaintextSize is not an exact value
     */
    boolean plaintextSizeCalculationIsAnEstimate();

    /**
     * Flag indicating if authentication is built into the cipher's design.
     *
     * @return true if AEAD cipher is supported
     */
    boolean isAEADCipher();

    /**
     * Creates the {@link AlgorithmParameterSpec} object needed for the associated
     * algorithm that is seeded with the passed initialization vector (IV).
     *
     * @param iv initialization vector
     * @return configured instance
     */
    AlgorithmParameterSpec getEncryptionParameterSpec(byte[] iv);

    /**
     * Creates a new instance of a HMAC calculating class that performs
     * authentication against the ciphertext.
     *
     * @return null if AEAD cipher, otherwise a new instance of authentication HMAC
     */
    HMac getAuthenticationHmac();

    /**
     * Translates a plaintext byte range to a ciphertext byte range with
     * skip modifier.
     *
     * @param startPositionInclusive starting position of byte range (0-Long.MAX)
     * @param endPositionInclusive ending position of byte range (-1, 0-Long.MAX)
     * @return object with the needed ciphertext numeric positions specified
     */
    ByteRangeConversion translateByteRange(long startPositionInclusive, long endPositionInclusive);

    /**
     * Updates a given {@link Cipher}'s state such that it can decrypt
     * data from a given position.
     *
     * @param cipher object to update
     * @param position position to update to
     * @return the number of bytes to skip from the plaintext output in
     *         order to get to the desired position
     */
    long updateCipherToPosition(Cipher cipher, long position);

    /**
     * Generates an initialization vector that is appropriate for the
     * cipher and cipher mode.
     *
     * @return an initialization vector in bytes
     */
    byte[] generateIv();

    /**
     * @return true when the cipher/mode supports reading from arbitrary positions
     */
    boolean supportsRandomAccess();

    /**
     * Finds a cipher by name and provider. Only throws runtime exceptions.
     *
     * @param cipherName Cipher name
     * @param provider provider name
     * @return Cipher instance
     * @throws MantaClientEncryptionException thrown if there is a problem getting the cipher
     */
    @SuppressWarnings("InsecureCryptoUsage")
    static Cipher findCipher(final String cipherName, final Provider provider) {
        /* We suppress the error-prone warning:
         * Insecure usage of a crypto API: the transformation is not a compile-time constant expression.
         *
         * This is a false positive because the cipher name and the provider
         * are hard-coded in each SupportedCipherDetails implementation. This
         * method just saves us from duplicating code. */

        try {
            return Cipher.getInstance(cipherName, provider);
        } catch (NoSuchAlgorithmException e) {
            String msg = String.format("Couldn't find algorithm [%s] via the "
                    + "[%s] provider", cipherName, provider.getName());
            throw new MantaClientEncryptionException(msg, e);
        } catch (NoSuchPaddingException e) {
            String[] split = cipherName.split("/");
            if (split.length >= 3) {
                String padding = split[2];
                String msg = String.format("Invalid padding mode specified: %s",
                        padding);
                throw new MantaClientEncryptionException(msg, e);
            } else {
                throw new MantaClientEncryptionException(e);
            }
        }
    }
}
