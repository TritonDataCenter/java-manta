/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import org.apache.http.entity.ContentType;
import org.testng.Assert;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;

public abstract class AbstractCipherDetailsTest {

        static final String TEXT = "A SERGEANT OF THE LAW, wary and wise, " +
                                   "That often had y-been at the Parvis, <26> " +
                                   "There was also, full rich of excellence. " +
                                   "Discreet he was, and of great reverence: " +
                                   "He seemed such, his wordes were so wise, " +
                                   "Justice he was full often in assize, " +
                                   "By patent, and by plein* commission; " +
                                   "For his science, and for his high renown, " +
                                   "Of fees and robes had he many one. " +
                                   "So great a purchaser was nowhere none. " +
                                   "All was fee simple to him, in effect " +
                                   "His purchasing might not be in suspect* " +
                                   "Nowhere so busy a man as he there was " +
                                   "And yet he seemed busier than he was " +
                                   "In termes had he case' and doomes* all " +
                                   "That from the time of King Will. were fall. " +
                                   "Thereto he could indite, and make a thing " +
                                   "There coulde no wight *pinch at* his writing. " +
                                   "And every statute coud* he plain by rote " +
                                   "He rode but homely in a medley* coat, " +
                                   "Girt with a seint* of silk, with barres small; " +
                                   "Of his array tell I no longer tale.";

    /**
     * Method to test plaintext to ciphertext byte position translation (start).
     *
     * @param start plaintext start
     * @param expected expected ciphertext byte position
     * @param map cipher position map
     */
    protected static void testPlainToCipherStart(final long start, final long expected, final CipherMap map) {

        final long actual = map.plainToCipherStart(start);

        Assert.assertEquals(actual, expected);
    }

    /**
     * Method to test plaintext to ciphertext byte position translation (end).
     *
     * @param end plaintext end
     * @param expected expected ciphertext byte position
     * @param map cipher position map
     */
    protected static void testPlainToCipherEnd(final long end, final long expected, final CipherMap map) {

        final long actual = map.plainToCipherEnd(end);

        Assert.assertEquals(actual, expected);
    }

    /**
     * Method to test plaintext ciphertext offset position translation.
     *
     * @param start plaintext start
     * @param expected expected ciphertext offset
     * @param map cipher position map
     */
    protected static void testPlainToCipherOffset(final long start, final long expected, final CipherMap map) {

        final long actual = map.plainToCipherOffset(start);

        Assert.assertEquals(actual, expected);
    }

    /**
     * Method to test a random read of a ciphertext.
     *
     * @param start plaintext start
     * @param end plaintext end
     * @param plaintextBytes plaintext bytes
     * @param ciphertextBytes ciphertext bytes
     * @param iv cipher initialization vector
     * @param key secret key
     * @param details cipher details
     *
     * @throws IOException if there is an error reading any data
     * @throws GeneralSecurityException if there is an error decrypting the ciphertext
     */
    private static void testRandomRead(final long start, final long end,
                                       final byte[] plaintextBytes, final byte[] ciphertextBytes,
                                       final byte[] iv, final SecretKey key, final SupportedCipherDetails details)
            throws IOException, GeneralSecurityException {

        final ClosedByteRange byteRange = new ClosedByteRange(start, end + 1);
        final EncryptedClosedByteRange<NullByteRange, OpenByteRange, ClosedByteRange>
                encryptedByteRange = new EncryptedClosedByteRange<>(byteRange, (RandomAccessCipher) details, details);

        final int plaintextStart = (int) byteRange.getStart();
        final int plaintextEnd = (int) byteRange.getEnd();
        final int plaintextLength = plaintextEnd - plaintextStart;

        final int ciphertextStart = (int) encryptedByteRange.getStart();
        final int ciphertextEnd = (int) encryptedByteRange.getEnd();
        final int ciphertextLength = ciphertextEnd - ciphertextStart;

        final int ciphertextOffset = (int) encryptedByteRange.getOffset();

        byte[] rangeBytes = Arrays.copyOfRange(plaintextBytes, plaintextStart, plaintextEnd);

        final Cipher cipher = details.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, details.getEncryptionParameterSpec(iv));

        details.updateCipherToPosition(cipher, ciphertextStart);

        byte[] ciphertextRangeBytes = Arrays.copyOfRange(ciphertextBytes, ciphertextStart, Math.min(ciphertextBytes.length, ciphertextEnd));

        byte[] decryptedBytes = cipher.doFinal(ciphertextRangeBytes);
        byte[] decryptedRangeBytes = Arrays.copyOfRange(decryptedBytes, ciphertextOffset, Math.min(decryptedBytes.length, ciphertextOffset + plaintextLength));

        String decryptedRangeText = new String(decryptedRangeBytes);
        String rangeText = new String(rangeBytes);

        Assert.assertEquals(decryptedRangeText, rangeText,
                "Random read output from ciphertext doesn't match expectation " +
                        "[cipherId=" + details.getCipherId() +
                        ", ciphertextStart=" + ciphertextStart +
                        ", ciphertextEnd=" + ciphertextEnd +
                        ", ciphertextLength=" + ciphertextLength +
                        ", ciphertextOffset=" + ciphertextOffset +
                        ", plaintextStart=" + plaintextStart +
                        ", plaintextEnd=" + plaintextEnd +
                        ", plaintextLength=" + plaintextLength +
                        ", plaintextBytes.length=" + plaintextBytes.length +
                        ", ciphertextBytes.length=" + ciphertextBytes.length +
                        ", decryptedBytes.length=" + decryptedBytes.length +
                        "]");
    }

    private static EncryptingEntity buildEncryptingEntity(final byte[] plaintext, final SecretKey key, final SupportedCipherDetails details) {

        final ExposedByteArrayEntity entity = new ExposedByteArrayEntity(plaintext, ContentType.APPLICATION_OCTET_STREAM);
        final EncryptingEntity encryptingEntity = new EncryptingEntity(key, details, entity);

        return encryptingEntity;
    }

    private static byte[] plainToCiphertext(final EncryptingEntity encryptor, final SupportedCipherDetails details) throws IOException {

        final byte[] ciphertext;

        final int macLength = (int) details.getAuthenticationTagOrHmacLengthInBytes();

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            encryptor.writeTo(out);

            final byte[] encrypted = out.toByteArray();

            ciphertext = Arrays.copyOf(encrypted, encrypted.length - macLength);
        }

        return ciphertext;
    }

    protected void sizeCalculationWorksRoundTrip(final SupportedCipherDetails cipherDetails,
                                                 final long plaintextSize) {
        long ciphertextSize = cipherDetails.ciphertextSize(plaintextSize);
        long calculatedPlaintextSize = cipherDetails.plaintextSize(ciphertextSize);

        if (cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            long difference = calculatedPlaintextSize - plaintextSize;
            Assert.assertTrue(difference < cipherDetails.getBlockSizeInBytes(),
                    "Plaintext size difference was greater than the block size");
        } else {
            Assert.assertEquals(calculatedPlaintextSize, plaintextSize,
                    "Size calculation didn't work for cipher: " + cipherDetails.getCipherId());
        }
    }

    protected void sizeCalculationWorksComparedToActualCipher(final SupportedCipherDetails cipherDetails) throws Exception {
        int arbitrarySize = 30475;
        byte[] plaintext = new byte[arbitrarySize];
        Arrays.fill(plaintext, (byte)0);

        Cipher cipher = cipherDetails.getCipher();
        byte[] iv = new byte[16];
        AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(iv);
        SecretKey key = SecretKeyUtils.generate(cipherDetails);
        cipher.init(Cipher.ENCRYPT_MODE, key, spec);

        byte[] ciphertext = cipher.doFinal(plaintext);

        final long ciphertextSize;

        if (cipherDetails.isAEADCipher()) {
            ciphertextSize = ciphertext.length;
        } else {
            ciphertextSize = ciphertext.length + cipherDetails.getAuthenticationTagOrHmacLengthInBytes();
        }

        long calculatedCiphertextSize = cipherDetails.ciphertextSize(plaintext.length);

        Assert.assertEquals(ciphertextSize, calculatedCiphertextSize,
                "Calculated ciphertext size didn't match actual ciphertext size " +
                        "for cipher: " + cipherDetails.getCipherId());

        long calculatedPlaintextSize = cipherDetails.plaintextSize(ciphertextSize);

        if (cipherDetails.plaintextSizeCalculationIsAnEstimate()) {
            long difference = calculatedPlaintextSize - plaintext.length;
            Assert.assertTrue(difference < cipherDetails.getBlockSizeInBytes(),
                    "Plaintext size difference was greater than the block size");
        } else {
            Assert.assertEquals(plaintext.length, calculatedPlaintextSize,
                    "Calculated plaintext size didn't match actual plaintext size " +
                            "for cipher: " + cipherDetails.getCipherId());
        }
    }

    protected void canRandomlyReadPlaintextPositionFromCiphertext(final SecretKey key,
                                                                  final SupportedCipherDetails details)
            throws IOException, GeneralSecurityException {

        final byte[] plaintextBytes = TEXT.getBytes(StandardCharsets.US_ASCII);

        if (!(details instanceof RandomAccessCipher)) {

            final String msg = "cipher doesn't support random access";
            throw new IllegalArgumentException(msg);
        }

        final EncryptingEntity encryptor = buildEncryptingEntity(plaintextBytes, key, details);

        final byte[] iv = encryptor.getCipher()
                                   .getIV();
        final byte[] ciphertextBytes = plainToCiphertext(encryptor, details);

        for (long start = 0; start < plaintextBytes.length - 1; start++) {

            for (long end = start; end < plaintextBytes.length - 1; end++) {

                testRandomRead(start, end, plaintextBytes, ciphertextBytes, iv, key, details);
            }

        }

    }

}
