/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.exception.MantaClientEncryptionException;

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
     * Map of all of the ciphers supported by the SDK indexed by algorithm name.
     */
    SupportedCiphersLookupMap SUPPORTED_CIPHERS = new SupportedCiphersLookupMap();

    /**
     * @return algorithm name used by key generation (e.g. {@link SecretKeyUtils#generate(String, int)})
     */
    String getKeyGenerationAlgorithm();

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
     * @param plainTextSize size of the plaintext input
     * @return size of the ciphertext output
     */
    long cipherTextSize(long plainTextSize);

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
     * Finds a cipher by name and provider. Only throws runtime exceptions.
     *
     * @param cipherName Cipher name
     * @param provider provider name
     * @return Cipher instance
     * @throws MantaClientEncryptionException thrown if there is a problem getting the cipher
     */
    static Cipher findCipher(final String cipherName, final Provider provider) {
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
