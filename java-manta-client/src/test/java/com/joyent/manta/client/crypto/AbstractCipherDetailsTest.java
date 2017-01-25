package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.testng.Assert;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;

public abstract class AbstractCipherDetailsTest {
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

    protected void canRandomlyReadPlaintextPositionFromCiphertext(final SecretKey secretKey,
                                                                  final SupportedCipherDetails cipherDetails)
            throws IOException, GeneralSecurityException {
        String text = "A SERGEANT OF THE LAW, wary and wise,\n" +
                "That often had y-been at the Parvis, <26>\n" +
                "There was also, full rich of excellence.\n" +
                "Discreet he was, and of great reverence:\n" +
                "He seemed such, his wordes were so wise,\n" +
                "Justice he was full often in assize,\n" +
                "By patent, and by plein* commission;\n" +
                "For his science, and for his high renown,\n" +
                "Of fees and robes had he many one.\n" +
                "So great a purchaser was nowhere none.\n" +
                "All was fee simple to him, in effect\n" +
                "His purchasing might not be in suspect*\n" +
                "Nowhere so busy a man as he there was\n" +
                "And yet he seemed busier than he was\n" +
                "In termes had he case' and doomes* all\n" +
                "That from the time of King Will. were fall.\n" +
                "Thereto he could indite, and make a thing\n" +
                "There coulde no wight *pinch at* his writing.\n" +
                "And every statute coud* he plain by rote\n" +
                "He rode but homely in a medley* coat,\n" +
                "Girt with a seint* of silk, with barres small;\n" +
                "Of his array tell I no longer tale.";

        byte[] plaintext = text.getBytes(Charset.forName("US-ASCII"));

        ContentType contentType = ContentType.APPLICATION_OCTET_STREAM;;
        ExposedByteArrayEntity entity = new ExposedByteArrayEntity(plaintext, contentType);
        EncryptingEntity encryptingEntity = new EncryptingEntity(secretKey, cipherDetails,
                entity);

        final byte[] ciphertext;
        final byte[] iv = encryptingEntity.getCipher().getIV();
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            encryptingEntity.writeTo(out);
            ciphertext = Arrays.copyOf(out.toByteArray(), out.toByteArray().length - cipherDetails.getAuthenticationTagOrHmacLengthInBytes());
        }

        long startPlaintextRange = 16;
        long endPlaintextRange = 129;

        byte[] adjustedPlaintext = Arrays.copyOfRange(plaintext,
                (int)startPlaintextRange, (int)(endPlaintextRange - startPlaintextRange));

        long[] ranges = cipherDetails.translateByteRange(startPlaintextRange, endPlaintextRange);
        long startCipherTextRange = ranges[0];
        long adjustedPlaintextRange = ranges[1];
        long endCipherTextRange = ranges[2];
        long adjustedPlaintextLength = ranges[3];

        Cipher decryptor = cipherDetails.getCipher();

        AlgorithmParameterSpec spec = calculateIVForOffset(
                (IvParameterSpec)cipherDetails.getEncryptionParameterSpec(iv),
                2, cipherDetails.getBlockSizeInBytes());

        decryptor.init(Cipher.DECRYPT_MODE, secretKey, spec);

        byte[] adjustedCipherText = Arrays.copyOfRange(ciphertext, (int)startCipherTextRange, (int)endCipherTextRange);
        byte[] out = decryptor.doFinal(adjustedCipherText);
        byte[] decrypted = Arrays.copyOfRange(out, (int)adjustedPlaintextRange, (int)adjustedPlaintextLength);

        String decryptedText = new String(decrypted);
        String adjustedText = new String(adjustedPlaintext);

        Assert.assertEquals(decryptedText, adjustedText,
                "Random read output from ciphertext doesn't match expectation " +
                        "[cipher=" + cipherDetails.getCipherId() + "]");
    }

    protected static IvParameterSpec calculateIVForOffset(final IvParameterSpec iv,
                                                          final long blockOffset,
                                                          final int blockSize) {
        final BigInteger ivBI = new BigInteger(1, iv.getIV());
        final BigInteger ivForOffsetBI = ivBI.add(BigInteger.valueOf(blockOffset
                / blockSize));

        final byte[] ivForOffsetBA = ivForOffsetBI.toByteArray();
        final IvParameterSpec ivForOffset;
        if (ivForOffsetBA.length >= blockSize) {
            ivForOffset = new IvParameterSpec(ivForOffsetBA, ivForOffsetBA.length - blockSize,
                    blockSize);
        } else {
            final byte[] ivForOffsetBASized = new byte[blockSize];
            System.arraycopy(ivForOffsetBA, 0, ivForOffsetBASized, blockSize
                    - ivForOffsetBA.length, ivForOffsetBA.length);
            ivForOffset = new IvParameterSpec(ivForOffsetBASized);
        }

        return ivForOffset;
    }
}
