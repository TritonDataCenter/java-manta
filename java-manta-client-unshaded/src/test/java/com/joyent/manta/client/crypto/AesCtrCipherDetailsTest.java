/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;

@Test
public class AesCtrCipherDetailsTest extends AbstractCipherDetailsTest {
    public void size1024bCalculationWorksRoundTripAes128() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes192() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size1024bCalculationWorksRoundTripAes256() {
        final long size = 1024;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size0bCalculationWorksRoundTripAes128() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes192() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size0bCalculationWorksRoundTripAes256() {
        final long size = 0;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void size2009125bCalculationWorksRoundTripAes128() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_128_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes192() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_192_BIT, size);
    }

    @Test(groups = {"unlimited-crypto"})
    public void size2009125bCalculationWorksRoundTripAes256() {
        final long size = 2009125;
        sizeCalculationWorksRoundTrip(AesCtrCipherDetails.INSTANCE_256_BIT, size);
    }

    public void ciphertextSizeCalculationWorksForAes128() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_128_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes192() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_192_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void ciphertextSizeCalculationWorksForAes256() throws Exception {
        sizeCalculationWorksComparedToActualCipher(AesCtrCipherDetails.INSTANCE_256_BIT);
    }

    @Test(groups = {"unlimited-crypto"})
    public void canQueryCiphertextByteRangeAes256() throws Exception {
        SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
        SupportedCipherDetails cipherDetails2 = AesCtrCipherDetails.INSTANCE_256_BIT;
        SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        canRandomlyReadPlaintextPositionFromCiphertext(secretKey, cipherDetails, cipherDetails2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void translateByteRangeThrowsWithoutStartInclusive() {
        AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(-1, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void translateByteRangeThrowsWithLargeStartInclusive() {
        AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(Long.MAX_VALUE, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void translateByteRangeThrowsWithoutEndInclusive() {
        AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(1, -1);
    }

    public void translateByteRangeReturnsCorrectRange() throws Exception {
        ByteRangeConversion byteRange1 = AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(5, 10);
        Assert.assertEquals(byteRange1.getCiphertextStartPositionInclusive(), 0);
        Assert.assertEquals(byteRange1.getCiphertextEndPositionInclusive(), 15);
        Assert.assertEquals(byteRange1.getPlaintextBytesToSkipInitially(), 5);
        Assert.assertEquals(byteRange1.getLengthOfPlaintextIncludingSkipBytes(), 11);

        ByteRangeConversion byteRange2 = AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(5, 22);
        Assert.assertEquals(byteRange2.getCiphertextStartPositionInclusive(), 0);
        Assert.assertEquals(byteRange2.getCiphertextEndPositionInclusive(), 31);
        Assert.assertEquals(byteRange2.getPlaintextBytesToSkipInitially(), 5);
        Assert.assertEquals(byteRange2.getLengthOfPlaintextIncludingSkipBytes(), 23);

        ByteRangeConversion byteRange3 = AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(32, 35);
        Assert.assertEquals(byteRange3.getCiphertextStartPositionInclusive(), 32);
        Assert.assertEquals(byteRange3.getCiphertextEndPositionInclusive(), 47);
        Assert.assertEquals(byteRange3.getPlaintextBytesToSkipInitially(), 0);
        Assert.assertEquals(byteRange3.getLengthOfPlaintextIncludingSkipBytes(), 4);

    }

    protected void canRandomlyReadPlaintextPositionFromCiphertext(final SecretKey secretKey,
                                                                  final SupportedCipherDetails cipherDetails,
                                                                  final SupportedCipherDetails cipherDetails2)
            throws IOException, GeneralSecurityException {
        String text = "A SERGEANT OF THE LAW, wary and wise, " +
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

        byte[] plaintext = text.getBytes(StandardCharsets.US_ASCII);

        ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;
        ExposedByteArrayEntity entity = new ExposedByteArrayEntity(plaintext, contentType);
        EncryptingEntity encryptingEntity = new EncryptingEntity(secretKey, cipherDetails,
                entity);

        final byte[] ciphertext;
        final byte[] iv = encryptingEntity.getCipher().getIV();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            encryptingEntity.writeTo(out);
            ciphertext = Arrays.copyOf(out.toByteArray(), out.toByteArray().length - cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        }

        for (long startPlaintextRange = 0; startPlaintextRange < plaintext.length - 1; startPlaintextRange++) {
            for (long endPlaintextRange = startPlaintextRange; endPlaintextRange < plaintext.length; endPlaintextRange++) {

                byte[] adjustedPlaintext = Arrays.copyOfRange(plaintext,
                        (int) startPlaintextRange, (int) endPlaintextRange + 1);

                ByteRangeConversion ranges = cipherDetails.translateByteRange(startPlaintextRange, endPlaintextRange);
                long startCipherTextRange = ranges.getCiphertextStartPositionInclusive();
                long endCipherTextRange = ranges.getCiphertextEndPositionInclusive();
                long adjustedPlaintextLength = ranges.getLengthOfPlaintextIncludingSkipBytes();

                // Decrypt using the old method of advancing the Cipher to the correct cipher block.
                // This method was correct, but had poor performance. It is still useful for testing to
                // verify the current calculation method is correct.
                Cipher decryptor = cipherDetails.getCipher();

                decryptor.init(Cipher.DECRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));
                long adjustedPlaintextRange = cipherDetails.updateCipherToPosition(decryptor, startPlaintextRange);

                byte[] adjustedCipherText = Arrays.copyOfRange(ciphertext, (int) startCipherTextRange, (int) Math.min(ciphertext.length, endCipherTextRange + 1));
                byte[] out = decryptor.doFinal(adjustedCipherText);
                byte[] decrypted = Arrays.copyOfRange(out, (int) adjustedPlaintextRange,
                        (int) Math.min(out.length, adjustedPlaintextLength));

                // Decrypt using the current method of advancing the Cipher to the correct cipher block
                Cipher decryptor2 = cipherDetails2.getCipher();
                AlgorithmParameterSpec spec = cipherDetails2.getEncryptionParameterSpec(iv, startCipherTextRange);
                decryptor2.init(Cipher.DECRYPT_MODE, secretKey, spec);

                byte[] out2 = decryptor2.doFinal(adjustedCipherText);
                byte[] decrypted2 = Arrays.copyOfRange(out, (int) adjustedPlaintextRange,
                        (int) Math.min(out2.length, adjustedPlaintextLength));

                String decryptedText = new String(decrypted, StandardCharsets.UTF_8);
                String decryptedText2 = new String(decrypted2, StandardCharsets.UTF_8);
                String adjustedText = new String(adjustedPlaintext, StandardCharsets.UTF_8);

                Assert.assertEquals(adjustedText, decryptedText,
                        "Random read output from ciphertext doesn't match expectation " +
                                "[cipher=" + cipherDetails.getCipherId() + "]");
                Assert.assertEquals(adjustedText, decryptedText2,
                        "Random read output from ciphertext doesn't match expectation " +
                                "[cipher=" + cipherDetails2.getCipherId() + "]");
                Assert.assertEquals(decryptedText, decryptedText2,
                        "Decrypted texts using different IV initialization methods do not match " +
                                "[cipher1=" + cipherDetails.getCipherId() +
                                " cipher12=" + cipherDetails2.getCipherId() + "]");
            }
        }
    }

    public void verifyIvParameterSpecIdentity() throws Exception {
        boolean done = false;
        int iterations = 0;
        final int maxIterations = 10000;
        while (!done && iterations++ < maxIterations) {
            SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_256_BIT;
            final byte[] iv = cipherDetails.generateIv();

            // Verify that calling getEncryptionParameterSpec with the IV and
            // a zero offset results in the same IV in the resulting output.
            // This serves as a regression test for an issue where this property
            // did not always hold and led to decryption issues. The problem was
            // specifically related to IV values whose BigInteger representation
            // was negative and for that reason this test iterates for up to
            // 10,000 iterations or until an IV whose BigInteger representation
            // is negative is generated.
            AlgorithmParameterSpec spec = cipherDetails.getEncryptionParameterSpec(iv, 0L);
            Cipher cipher = cipherDetails.getCipher();
            SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, spec);
            Assert.assertEquals(iv, cipher.getIV(), "Calculated initialization " +
                    "vector does not match expected value [expected=" + Arrays.toString(iv) +
                    " actual=" + Arrays.toString(cipher.getIV()) + "]");

            // Calculate BigInteger value of IV
            final BigInteger ivBigInt = new BigInteger(iv);
            if (ivBigInt.compareTo(BigInteger.ZERO) < 0) {
                done = true;
            }
        }
    }
}
