/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.bouncycastle.crypto.macs.HMac;

import javax.crypto.Cipher;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Implementation of an AESCipherDetails that MUST NOT be used within the current runtime.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 */
public class LocallyIllegalAesCipherDetails implements SupportedCipherDetails {

    /**
     * Size of the private key - which determines the AES algorithm type.
     */
    private final int keyLengthBits;

    /**
     * Package private constructor for LocallyIllegalAesCipherDetails.
     *
     * @param keyLengthBits size of the secret key
     */
    LocallyIllegalAesCipherDetails(final int keyLengthBits) {
        this.keyLengthBits = keyLengthBits;
    }

    /**
     * Fail immediately, we don't belong on this JVM. We throw an Error instead of an Exception because it is unlikely
     * the runtime could recover from a missing extension. Additionally, Errors are unchecked exceptions so they don't
     * require callers to change their method signatures.
     */
    private void fail() {
        throw new Error("This cipher is not compatible with the current runtime: (keyLengthBits="
                + this.keyLengthBits + ", maxKeyLength=" + AesCipherDetailsFactory.MAX_KEY_LENGTH_ALLOWED);
    }

    @Override
    public String getKeyGenerationAlgorithm() {
        fail();
        return null;
    }

    @Override
    public String getCipherId() {
        fail();
        return null;
    }

    @Override
    public String getCipherAlgorithm() {
        fail();
        return null;
    }

    @Override
    public int getKeyLengthBits() {
        fail();
        return 0;
    }

    @Override
    public int getBlockSizeInBytes() {
        fail();
        return 0;
    }

    @Override
    public int getIVLengthInBytes() {
        fail();
        return 0;
    }

    @Override
    public int getAuthenticationTagOrHmacLengthInBytes() {
        fail();
        return 0;
    }

    @Override
    public long getMaximumPlaintextSizeInBytes() {
        fail();
        return 0L;
    }

    @Override
    public Cipher getCipher() {
        fail();
        return null;
    }

    @Override
    public long ciphertextSize(final long plaintextSize) {
        fail();
        return 0L;
    }

    @Override
    public long plaintextSize(final long ciphertextSize) {
        fail();
        return 0L;
    }

    @Override
    public boolean plaintextSizeCalculationIsAnEstimate() {
        fail();
        return false;
    }

    @Override
    public boolean isAEADCipher() {
        fail();
        return false;
    }

    @Override
    public AlgorithmParameterSpec getEncryptionParameterSpec(final byte[] iv) {
        fail();
        return null;
    }

    @Override
    public HMac getAuthenticationHmac() {
        fail();
        return null;
    }

    @Override
    public ByteRangeConversion translateByteRange(final long startPositionInclusive, final long endPositionInclusive) {
        fail();
        return null;
    }

    @Override
    public long updateCipherToPosition(final Cipher cipher, final long position) {
        fail();
        return 0L;
    }

    @Override
    public byte[] generateIv() {
        fail();
        return null;
    }

    @Override
    public boolean supportsRandomAccess() {
        fail();
        return false;
    }

}
