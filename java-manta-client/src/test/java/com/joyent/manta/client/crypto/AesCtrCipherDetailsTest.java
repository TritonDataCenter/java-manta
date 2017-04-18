/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.ClosedByteRange;
import com.joyent.manta.client.NullByteRange;
import com.joyent.manta.client.OpenByteRange;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;

import org.apache.http.entity.ContentType;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Arrays;

@Test
public class AesCtrCipherDetailsTest extends AbstractCipherDetailsTest {

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
    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    public void canQueryCiphertextByteRangeAes128() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
    }

    public void canQueryCiphertextByteRangeAes192() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_192_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
    }

    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails);
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
