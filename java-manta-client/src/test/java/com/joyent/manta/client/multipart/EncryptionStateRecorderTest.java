package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.AesCbcCipherDetails;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

public class EncryptionStateRecorderTest {

    @DataProvider(name = "supportedCiphers")
    public Object[][] supportedCiphers() {
        ArrayList<SupportedCipherDetails> ciphers = new ArrayList<>();

        // only AES/CTR is supported by MPU right now
        // but we can test against all of them anyways
        ciphers.add(AesGcmCipherDetails.INSTANCE_128_BIT);
        ciphers.add(AesGcmCipherDetails.INSTANCE_192_BIT);
        ciphers.add(AesGcmCipherDetails.INSTANCE_256_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_128_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_192_BIT);
        ciphers.add(AesCtrCipherDetails.INSTANCE_256_BIT);
        ciphers.add(AesCbcCipherDetails.INSTANCE_128_BIT);
        ciphers.add(AesCbcCipherDetails.INSTANCE_192_BIT);
        ciphers.add(AesCbcCipherDetails.INSTANCE_256_BIT);

        ArrayList<Object[]> params = new ArrayList<>();

        for (SupportedCipherDetails cipher : ciphers) {
            params.add(new Object[]{cipher});
        }

        return params.toArray(new Object[][]{});
    }

    @Test(dataProvider = "supportedCiphers")
    public void testRecordAndRewindFirstPart(final SupportedCipherDetails cipherDetails) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        // cipher IV prepared in EncryptionContext constructor
        final EncryptionContext ctx = new EncryptionContext(secretKey, cipherDetails);
        final EncryptionState state = new EncryptionState(ctx);
        final byte[] iv = ctx.getCipher().getIV();

        initializeEncryptionStateStreams(state);

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

    @Test(dataProvider = "supportedCiphers")
    public void testRecordAndRewindSinglePartWithEntity(final SupportedCipherDetails cipherDetails) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final EncryptionContext ctx = new EncryptionContext(secretKey, cipherDetails);
        final byte[] iv = ctx.getCipher().getIV();
        final EncryptionState state = new EncryptionState(ctx);

        // entity will always finalize encryption
        final EncryptingPartEntity.LastPartCallback finalizingCallback = new EncryptingPartEntity.LastPartCallback() {
            @Override
            public ByteArrayOutputStream call(final long uploadedBytes) throws IOException {
                return state.remainderAndLastPartAuth();
            }
        };

        initializeEncryptionStateStreams(state);

        final int inputSize = RandomUtils.nextInt(1000, 2000);
        final byte[] content = RandomUtils.nextBytes(inputSize);

        final EncryptionStateSnapshot snapshot = EncryptionStateRecorder.record(state, null);

        final byte[] lostBytes = writeEncryptedBytePart(state, content, finalizingCallback);

        // rewind state so the next entity can use it
        EncryptionStateRecorder.rewind(state, snapshot);

        final byte[] uploadedBytes = writeEncryptedBytePart(state, content, finalizingCallback);

        // verify the parts encrypt to the same ciphertext
        AssertJUnit.assertArrayEquals(lostBytes, uploadedBytes);

        // check original plaintext against uploaded ciphertext
        validateCiphertext(cipherDetails, secretKey, content, iv, uploadedBytes);
    }

    @Test(dataProvider = "supportedCiphers")
    public void testRecordAndRewindMultipleParts(final SupportedCipherDetails cipherDetails) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final EncryptionContext ctx = new EncryptionContext(secretKey, cipherDetails);
        final byte[] iv = ctx.getCipher().getIV();
        final EncryptionState state = new EncryptionState(ctx);

        // prepare part content
        final int inputSize = RandomUtils.nextInt(1000, 2000);
        final int firstPartSize = RandomUtils.nextInt(500, 999);

        final byte[] content = RandomUtils.nextBytes(inputSize);
        final byte[] content1 = Arrays.copyOfRange(content, 0, firstPartSize);
        final byte[] content2 = Arrays.copyOfRange(content, firstPartSize, content.length);

        initializeEncryptionStateStreams(state);

        final byte[] firstUpload = writeEncryptedBytePart(state, content1, null);

        final EncryptionStateSnapshot snapshot = EncryptionStateRecorder.record(state, null);

        final EncryptingPartEntity.LastPartCallback finalizingCallback = new EncryptingPartEntity.LastPartCallback() {
            @Override
            public ByteArrayOutputStream call(final long uploadedBytes) throws IOException {
                return state.remainderAndLastPartAuth();
            }
        };

        final byte[] lastUpload = writeEncryptedBytePart(state, content2, finalizingCallback);

        // attempting to resend the last part without rewinding should throw
        Assert.assertThrows(Exception.class, () -> {
            /*
            We are expecting a generic exception here because certain combinations of part sizes don't
            actually result in writes making it all the way to the CloseShieldOutputStream and triggering an IOException.

            This has been reported upstream in https://issues.apache.org/jira/browse/IO-546

            In _most_ cases an IOException will be thrown when the CipherOutputStream attempts to write data to the
            underlying CloseShieldOutputStream. In _some cases_ where the last part size is less than the cipher block size
            (e.g. inputSize = 1000, firstPartSize = 990) calling CipherOutputStream#write does not actually result in any
            bytes being written. We would expect an IOException to be thrown on `CipherOutputStream#flush` but
            `ClosedOutputStream#flush` will always silently succeed. This has been reported to
            Commons IO https://issues.apache.org/jira/browse/IO-546

            Additionally, AES/GCM will throw `IllegalStateException: GCM cipher cannot be reused for encryption`
             */
            writeEncryptedBytePart(state, content2, finalizingCallback);
        });

        EncryptionStateRecorder.rewind(state, snapshot);

        final byte[] lastRetryUpload = writeEncryptedBytePart(state, content2, finalizingCallback);

        AssertJUnit.assertArrayEquals(lastUpload, lastRetryUpload);

        final byte[] ciphertext = ArrayUtils.addAll(firstUpload, lastRetryUpload);
        validateCiphertext(cipherDetails, secretKey, content, iv, ciphertext);
    }

    // TEST UTILITY METHODS

    private void initializeEncryptionStateStreams(final EncryptionState state) {
        state.setMultipartStream(
                new MultipartOutputStream(
                        state.getEncryptionContext().getCipherDetails().getBlockSizeInBytes()));
        state.setCipherStream(
                EncryptingEntityHelper.makeCipherOutputForStream(
                        state.getMultipartStream(),
                        state.getEncryptionContext()));
    }

    /**
     * Create an {@link EncryptingPartEntity} using the provided {@link EncryptionState}, byte content and
     * {@link EncryptingPartEntity.LastPartCallback}.
     * The callback can be used to finalize encryption whenever necessary.
     */
    private byte[] writeEncryptedBytePart(EncryptionState state,
                                          byte[] content,
                                          EncryptingPartEntity.LastPartCallback callback) throws IOException {
        final EncryptingPartEntity lastEntity = new EncryptingPartEntity(
                state.getCipherStream(),
                state.getMultipartStream(),
                new ByteArrayEntity(content, ContentType.APPLICATION_OCTET_STREAM),
                callback
        );

        final ByteArrayOutputStream lastOutput = new ByteArrayOutputStream();
        state.getLock().lock();
        lastEntity.writeTo(lastOutput);
        state.getLock().unlock();

        return lastOutput.toByteArray();
    }

    private void validateCiphertext(SupportedCipherDetails cipherDetails,
                                    SecretKey secretKey,
                                    byte[] plaintext,
                                    byte[] iv,
                                    byte[] ciphertext) throws IOException {
        MantaHttpHeaders responseHttpHeaders = new MantaHttpHeaders();
        responseHttpHeaders.setContentLength((long) ciphertext.length);

        if (!cipherDetails.isAEADCipher()) {
            responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE,
                    SupportedHmacsLookupMap.hmacNameFromInstance(cipherDetails.getAuthenticationHmac()));
        }
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(iv));
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_KEY_ID,
                cipherDetails.getCipherId());

        final MantaEncryptedObjectInputStream decryptingStream = new MantaEncryptedObjectInputStream(
                new MantaObjectInputStream(
                        new MantaObjectResponse("/path", responseHttpHeaders),
                        Mockito.mock(CloseableHttpResponse.class),
                        new EofSensorInputStream(
                                new ByteArrayInputStream(ciphertext),
                                null)),
                cipherDetails,
                secretKey,
                true);

        ByteArrayOutputStream decrypted = new ByteArrayOutputStream();
        IOUtils.copy(decryptingStream, decrypted);
        decryptingStream.close();
        AssertJUnit.assertArrayEquals(plaintext, decrypted.toByteArray());
    }
}