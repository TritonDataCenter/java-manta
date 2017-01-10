/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Class that provides details about how the AES-GCM cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesGcmCipherDetails implements SupportedCipherDetails {
    /**
     * The maximum number of 16 byte blocks that can be encrypted with a
     * AES GCM cipher: 2^32 - 2.
     */
    private static final long MAX_AES_GCM_BLOCKS = 4294967294L;

    /**
     * The maximum size in bytes allowed for a plain-text input to the AES-GCM
     * cipher.
     */
    private static final long MAX_PLAIN_TEXT_SIZE_BYTES = MAX_AES_GCM_BLOCKS << 4;

    /**
     * Global instance of AES-GCM cipher.
     */
    public static final AesGcmCipherDetails INSTANCE = new AesGcmCipherDetails();

    /**
     * Creates a new instance of a AES-GCM cipher for the static instance.
     */
    private AesGcmCipherDetails() {
    }

    @Override
    public String getKeyGenerationAlgorithm() {
        return "AES";
    }

    @Override
    public String getCipherAlgorithm() {
        return "AES/GCM/NoPadding";
    }

    @Override
    public int getKeyLengthBits() {
        return 256;
    }

    @Override
    public int getBlockSizeInBytes() {
        return 16;
    }

    @Override
    public int getIVLengthInBytes() {
        return 12;
    }

    @Override
    public int getAuthenticationTagOrHmacLengthInBytes() {
        return 16; // 128 bits
    }

    @Override
    public long getMaximumPlaintextSizeInBytes() {
        return MAX_PLAIN_TEXT_SIZE_BYTES;
    }

    @Override
    public Cipher getCipher() {
        return SupportedCipherDetails.findCipher(getCipherAlgorithm(),
                BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
    }

    @Override
    public AlgorithmParameterSpec getEncryptionParameterSpec(final byte[] iv) {
        Validate.notNull(iv, "Initialization vector must not be null");
        Validate.isTrue(iv.length == getIVLengthInBytes(),
                "Initialization vector has the wrong byte count [%d] "
                + "expected [%d] bytes", iv.length, getIVLengthInBytes());

        int tagSizeInBits = getAuthenticationTagOrHmacLengthInBytes() << 3;
        return new GCMParameterSpec(tagSizeInBits, iv);
    }

    @Override
    public long cipherTextSize(final long plainTextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plainTextSize);
        return plainTextSize + getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public boolean isAEADCipher() {
        return true;
    }
}
