package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.Validate;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Class that provides details about how the AES-CTR cipher's settings.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public final class AesCtrCipherDetails implements SupportedCipherDetails {

    /**
     * Global instance of AES-CTR cipher.
     */
    public static final AesCtrCipherDetails INSTANCE = new AesCtrCipherDetails();

    /**
     * Creates a new instance of a AES-CTR cipher for the static instance.
     */
    private AesCtrCipherDetails() {
    }

    @Override
    public String getKeyGenerationAlgorithm() {
        return "AES";
    }

    @Override
    public String getCipherAlgorithm() {
        return "AES/CTR/NoPadding";
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
        return 16;
    }

    @Override
    public int getAuthenticationTagOrHmacLengthInBytes() {
        return 16; // 128 bits
    }

    @Override
    public long getMaximumPlaintextSizeInBytes() {
        return Long.MAX_VALUE;
    }

    @Override
    public Cipher getCipher() {
        return SupportedCipherDetails.findCipher(getCipherAlgorithm(),
                BouncyCastleLoader.BOUNCY_CASTLE_PROVIDER);
    }

    @Override
    public long cipherTextSize(final long plainTextSize) {
        Validate.inclusiveBetween(0L, Long.MAX_VALUE, plainTextSize);
        return plainTextSize;
    }


    @Override
    public boolean isAEADCipher() {
        return false;
    }

    @Override
    public AlgorithmParameterSpec getEncryptionParameterSpec(final byte[] iv) {
        Validate.notNull(iv, "Initialization vector must not be null");
        Validate.isTrue(iv.length == getIVLengthInBytes(),
                "Initialization vector has the wrong byte count [%d] "
                        + "expected [%d] bytes", iv.length, getIVLengthInBytes());

        return new IvParameterSpec(iv);
    }
}
