package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.util.Arrays;

@Test
public class AesCtrCipherDetailsTest extends AbstractCipherDetailsTest {
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
        Assert.assertEquals(byteRange1.getCiphertextEndPositionInclusive(), 32);
        Assert.assertEquals(byteRange1.getPlaintextBytesToSkipInitially(), 4);
        Assert.assertEquals(byteRange1.getLengthOfPlaintextIncludingSkipBytes(), 6);

        ByteRangeConversion byteRange2 = AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(5, 22);
        Assert.assertEquals(byteRange2.getCiphertextStartPositionInclusive(), 0);
        Assert.assertEquals(byteRange2.getCiphertextEndPositionInclusive(), 32);
        Assert.assertEquals(byteRange2.getPlaintextBytesToSkipInitially(), 4);
        Assert.assertEquals(byteRange2.getLengthOfPlaintextIncludingSkipBytes(), 18);

        ByteRangeConversion byteRange3 = AesCtrCipherDetails.INSTANCE_128_BIT.translateByteRange(32, 35);
        Assert.assertEquals(byteRange3.getCiphertextStartPositionInclusive(), 32);
        Assert.assertEquals(byteRange3.getCiphertextEndPositionInclusive(), 48);
        Assert.assertEquals(byteRange3.getPlaintextBytesToSkipInitially(), 0);
        Assert.assertEquals(byteRange3.getLengthOfPlaintextIncludingSkipBytes(), 4);
    }

    protected void canRandomlyReadPlaintextPositionFromCiphertext(final SecretKey secretKey,
                                                                  final SupportedCipherDetails cipherDetails)
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

        byte[] plaintext = text.getBytes(Charset.forName("US-ASCII"));

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

        for (long startPlaintextRange = 1; startPlaintextRange < plaintext.length - 1; startPlaintextRange++) {
            for (long endPlaintextRange = startPlaintextRange + 1; endPlaintextRange < plaintext.length; endPlaintextRange++) {

                byte[] adjustedPlaintext = Arrays.copyOfRange(plaintext,
                        (int) startPlaintextRange, (int) endPlaintextRange + 1);

                ByteRangeConversion ranges = cipherDetails.translateByteRange(startPlaintextRange, endPlaintextRange);
                long startCipherTextRange = ranges.getCiphertextStartPositionInclusive();
                long endCipherTextRange = ranges.getCiphertextEndPositionInclusive();
                long adjustedPlaintextLength = ranges.getLengthOfPlaintextIncludingSkipBytes();

                Cipher decryptor = cipherDetails.getCipher();

                decryptor.init(Cipher.DECRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));
                long adjustedPlaintextRange = cipherDetails.updateCipherToPosition(decryptor, startPlaintextRange);

                byte[] adjustedCipherText = Arrays.copyOfRange(ciphertext, (int) startCipherTextRange, (int) endCipherTextRange);
                byte[] out = decryptor.doFinal(adjustedCipherText);
                byte[] decrypted = Arrays.copyOfRange(out, (int) adjustedPlaintextRange,
                        (int) adjustedPlaintextLength + (int) adjustedPlaintextRange);

                String decryptedText = new String(decrypted);
                String adjustedText = new String(adjustedPlaintext);

                Assert.assertEquals(decryptedText, adjustedText,
                        "Random read output from ciphertext doesn't match expectation " +
                                "[cipher=" + cipherDetails.getCipherId() + "]");
            }
        }
    }
}
