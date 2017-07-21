package com.joyent.manta.client.multipart;

import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

/**
 * Created by tomascelaya on 7/21/17.
 */
public class EncryptionStateRecorderTest {

    @DataProvider(name = "supportedCiphersAndHmacs")
    public Object[][] supportedCiphersAndHmacs() {
        ArrayList<SupportedCipherDetails> ciphers = new ArrayList<>();

        // only AES/CTR is supported by MPU right now
        // ciphers.add(AesGcmCipherDetails.INSTANCE_128_BIT);
        // ciphers.add(AesGcmCipherDetails.INSTANCE_192_BIT);
        // ciphers.add(AesGcmCipherDetails.INSTANCE_256_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_128_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_192_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_256_BIT);
        // ciphers.add(AesCbcCipherDetails.INSTANCE_128_BIT);
        // ciphers.add(AesCbcCipherDetails.INSTANCE_192_BIT);
        // ciphers.add(AesCbcCipherDetails.INSTANCE_256_BIT);

        ArrayList<String> hmacNames = new ArrayList<>();
        hmacNames.add("HmacMD5");
        hmacNames.add("HmacSHA1");
        hmacNames.add("HmacSHA256");
        hmacNames.add("HmacSHA384");
        hmacNames.add("HmacSHA512");

        ArrayList<Object[]> params = new ArrayList<>();

        for (SupportedCipherDetails cipher : ciphers) {
            for (String hmacName : hmacNames) {
                params.add(new Object[]{cipher, hmacName});
            }
        }


        Object[][] arr = params.toArray(new Object[][]{});
        return arr;
    }

    @Test(dataProvider = "supportedCiphersAndHmacs")
    public void testRecordAndRewindFirstPart(final SupportedCipherDetails cipherDetails, final String hmacName) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        // cipher IV prepared in EncryptionContext constructor
        final EncryptionContext ctx = new EncryptionContext(secretKey, cipherDetails);
        final EncryptionState state = new EncryptionState(ctx);
        final byte[] iv = ctx.getCipher().getIV();

        prepareEncryptionState(state);

        final EncryptionStateSnapshot snapshot = EncryptionStateRecorder.record(state, null);
        Assert.assertNotSame(snapshot.getCipher(), state.getEncryptionContext().getCipher());

        final ByteArrayOutputStream originalOutput = new ByteArrayOutputStream();
        final ByteArrayOutputStream snapshotOutput = new ByteArrayOutputStream();

        final int inputSize = RandomUtils.nextInt(300, 600);
        final byte[] content = RandomUtils.nextBytes(inputSize);

        // encrypt to originalOutput through state.getCipherStream()
        state.getMultipartStream().setNext(originalOutput);
        IOUtils.copy(new ByteArrayInputStream(content), state.getCipherStream());
        // grab any final bytes that didn't fit into the block boundary
        originalOutput.write(ctx.getCipher().doFinal());

        // this is where the magic happens
        EncryptionStateRecorder.rewind(state, snapshot);

        // encrypt to snapshotOutput through state.getCipherStream()
        state.getMultipartStream().setNext(snapshotOutput);
        IOUtils.copy(new ByteArrayInputStream(content), state.getCipherStream());
        state.getCipherStream().flush();
        // grab any final bytes that didn't fit into the block boundary
        snapshotOutput.write(ctx.getCipher().doFinal());

        // verify the parts encrypt to the same ciphertext
        AssertJUnit.assertArrayEquals(originalOutput.toByteArray(), snapshotOutput.toByteArray());

        // grab a distinct Cipher object to decrypt
        final Cipher decryptCipher = cipherDetails.getCipher();
        decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));
        final byte[] originalDecrypted = decryptCipher.doFinal(originalOutput.toByteArray());
        final byte[] snapshotDecrypted = decryptCipher.doFinal(snapshotOutput.toByteArray());

        AssertJUnit.assertArrayEquals(content, originalDecrypted);
        AssertJUnit.assertArrayEquals(content, snapshotDecrypted);

        // TODO: this test does not validate hmac cloning yet, that is currently only covered by HmacClonerTest
    }

    private void prepareEncryptionState(final EncryptionState state) {
        state.setMultipartStream(
                new MultipartOutputStream(
                        state.getEncryptionContext().getCipherDetails().getBlockSizeInBytes()));
        state.setCipherStream(
                EncryptingEntityHelper.makeCipherOutputForStream(
                        state.getMultipartStream(),
                        state.getEncryptionContext()));
    }
}