/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Base implementation providing common functionality for all AES ciphers.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public abstract class AbstractAesCipherDetails implements SupportedCipherDetails {
    /**
     * Time interval in which to refresh the seed of the entropy source.
     */
    private static final Duration SEED_REFRESH_INTERVAL = Duration.ofHours(1);

    /**
     * Default HMAC algorithm to use for AES ciphers.
     */
    protected static final String DEFAULT_HMAC_ALGORITHM = "HmacMD5";

    /**
     * HMAC algorithm identifier.
     */
    private final String hmacAlgorithm;

    /**
     * The size of the HMAC signature or AEAD tag in bytes.
     */
    private final int authenticationTagOrHmacLength;

    /**
     * Size of the private key - which determines the AES algorithm type.
     */
    private final int keyLengthBits;

    /**
     * Cipher identifier in Manta.
     */
    private final String cipherId;

    /**
     * Cipher identifier used to get the cipher via JCE.
     */
    private final String cipherAlgorithmJavaName;

    /**
     * Flag indicating if the cipher uses AEAD.
     */
    private final boolean isAEADCipher;

    /**
     * Source of entropy for the encryption algorithm.
     * We use the JVM's {@link SecureRandom} because it
     * is configurable by the user.
     */
    private final SecureRandom random = findSecureRandomImplementation();

    /**
     * Timestamp in which the entropy's source was last refreshed with a
     * new seed value.
     */
    private volatile Instant seedLastRefreshedTimestamp = Instant.now();

    /**
     * Creates a new instance for a AEAD cipher.
     *
     * @param keyLengthBits size of the secret key
     * @param cipherAlgorithmJavaName identifier used to get the cipher via JCE
     * @param authenticationTagLength size of AEAD tag
     */
    public AbstractAesCipherDetails(final int keyLengthBits,
                                    final String cipherAlgorithmJavaName,
                                    final int authenticationTagLength) {
        this.keyLengthBits = keyLengthBits;
        this.cipherAlgorithmJavaName = cipherAlgorithmJavaName;
        this.cipherId = createMantaCipherIdFromJavaAlgorithmId(
                cipherAlgorithmJavaName, keyLengthBits);
        this.hmacAlgorithm = null;
        this.authenticationTagOrHmacLength = authenticationTagLength;
        this.isAEADCipher = true;
    }

    /**
     * Creates a new instance for a cipher authenticated by a HMAC.
     *
     * @param keyLengthBits size of the secret key
     * @param cipherAlgorithmJavaName identifier used to get the cipher via JCE
     * @param hmacAlgorithm HMAC algorithm to use for authentication
     */
    public AbstractAesCipherDetails(final int keyLengthBits,
                                    final String cipherAlgorithmJavaName,
                                    final String hmacAlgorithm) {
        this.keyLengthBits = keyLengthBits;
        this.cipherAlgorithmJavaName = cipherAlgorithmJavaName;
        this.cipherId = createMantaCipherIdFromJavaAlgorithmId(
                cipherAlgorithmJavaName, keyLengthBits);
        this.hmacAlgorithm = hmacAlgorithm;
        this.authenticationTagOrHmacLength = getAuthenticationHmac().getMacSize();
        this.isAEADCipher = false;
    }

    @Override
    public String getKeyGenerationAlgorithm() {
        return "AES";
    }

    @Override
    public long getMaximumPlaintextSizeInBytes() {
        return Long.MAX_VALUE - getAuthenticationTagOrHmacLengthInBytes();
    }

    @Override
    public int getAuthenticationTagOrHmacLengthInBytes() {
        return this.authenticationTagOrHmacLength;
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
    public int getKeyLengthBits() {
        return this.keyLengthBits;
    }

    @Override
    public String getCipherId() {
        return this.cipherId;
    }

    @Override
    public String getCipherAlgorithm() {
        return this.cipherAlgorithmJavaName;
    }

    @Override
    public Cipher getCipher() {
        if (ExternalSecurityProviderLoader.getPkcs11Provider() == null) {
            return SupportedCipherDetails.findCipher(cipherAlgorithmJavaName,
                    ExternalSecurityProviderLoader.getBouncyCastleProvider());
        }

        final Provider provider;

        if (ExternalSecurityProviderLoader.getPkcs11Provider().containsKey(
                "Cipher." + cipherAlgorithmJavaName)) {
            provider = ExternalSecurityProviderLoader.getPkcs11Provider();
        } else {
            provider = ExternalSecurityProviderLoader.getBouncyCastleProvider();
        }

        return SupportedCipherDetails.findCipher(cipherAlgorithmJavaName,
                provider);
    }

    @Override
    public Cipher getBouncyCastleCipher() {
        return SupportedCipherDetails.findCipher(cipherAlgorithmJavaName,
                ExternalSecurityProviderLoader.getBouncyCastleProvider());
    }

    @Override
    public HMac getAuthenticationHmac() {
        if (this.isAEADCipher) {
            return null;
        }

        return SupportedHmacsLookupMap.INSTANCE.get(hmacAlgorithm).get();
    }

    @Override
    public AlgorithmParameterSpec getEncryptionParameterSpec(final byte[] iv) {
        Validate.notNull(iv, "Initialization vector must not be null");
        Validate.isTrue(iv.length == getIVLengthInBytes(),
                "Initialization vector has the wrong byte count [%d] "
                        + "expected [%d] bytes", iv.length, getIVLengthInBytes());

        return new IvParameterSpec(iv);
    }

    /**
     * Parse the Java algorithm name for a cipher and then creates a Manta cipher
     * id compatible string.
     *
     * @param algorithm algorithm name in for form of: cipher/mode/padding
     * @param keyLengthBits number of bits used in the secret key
     * @return Manta cipher id like AES256/CBC/PKCS5Padding
     */
    private static String createMantaCipherIdFromJavaAlgorithmId(
            final String algorithm, final int keyLengthBits) {
        final char separator = '/';
        String[] parts = StringUtils.split(algorithm, separator);

        if (parts.length < 3) {
            throw new IllegalArgumentException("There must be three slashes [/] "
                    + "in the algorithm name");
        }

        return parts[0] + keyLengthBits + separator + parts[1] + separator + parts[2];
    }

    @Override
    public boolean isAEADCipher() {
        return this.isAEADCipher;
    }

    @Override
    public byte[] generateIv() {
        byte[] iv = new byte[getIVLengthInBytes()];
        getSecureRandom().nextBytes(iv);

        return iv;
    }

    /**
     * <p>Gets the current instance of {@link SecureRandom} and periodically adds
     * new random material to the seed.</p>
     * @see <a href="https://www.cigital.com/blog/proper-use-of-javas-securerandom/">Proper Use Of Java SecureRandom</a>
     *
     * @return entropy source
     */
    protected SecureRandom getSecureRandom() {
        Instant nextRefreshTimestamp = seedLastRefreshedTimestamp.plus(SEED_REFRESH_INTERVAL);
        if (seedLastRefreshedTimestamp.isAfter(nextRefreshTimestamp)) {
            this.random.setSeed(this.random.generateSeed(32));
            seedLastRefreshedTimestamp = Instant.now();
        }

        return this.random;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractAesCipherDetails that = (AbstractAesCipherDetails)o;

        return authenticationTagOrHmacLength == that.authenticationTagOrHmacLength
                && keyLengthBits == that.keyLengthBits
                && isAEADCipher == that.isAEADCipher
                && Objects.equals(hmacAlgorithm, that.hmacAlgorithm)
                && Objects.equals(cipherId, that.cipherId)
                && Objects.equals(cipherAlgorithmJavaName, that.cipherAlgorithmJavaName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hmacAlgorithm, authenticationTagOrHmacLength,
                keyLengthBits, cipherId, cipherAlgorithmJavaName, isAEADCipher);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("hmacAlgorithm", hmacAlgorithm)
                .append("authenticationTagOrHmacLength", authenticationTagOrHmacLength)
                .append("keyLengthBits", keyLengthBits)
                .append("cipherId", cipherId)
                .append("cipherAlgorithmJavaName", cipherAlgorithmJavaName)
                .append("isAEADCipher", isAEADCipher)
                .toString();
    }

    /**
     * This method attempts to find our first choice for a source of a source
     * of entropy and then chooses the default if that choice is not available.
     *
     * @return specific implementation of {@link SecureRandom}
     */
    private static SecureRandom findSecureRandomImplementation() {
        // First we attempt to a non-blocking source of entropy that typically
        // reads from /dev/urandom
        try {
            return SecureRandom.getInstance("NativePRNGNonBlocking", "SUN");
        } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            // We are here if we were unable to load that source of entropy
            // so we go with the default value
            return new SecureRandom();
        }
    }
}
