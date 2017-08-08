package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.EncryptingEntityHelper;
import com.joyent.manta.client.crypto.EncryptingPartEntity;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
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
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

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

    @Test(dataProvider = "supportedCiphersAndHmacs")
    public void testRecordAndRewindLastPartPartWithEntity(final SupportedCipherDetails cipherDetails, final String hmacName) throws Exception {
        final SecretKey secretKey = SecretKeyUtils.generate(cipherDetails);
        final EncryptionContext ctx = new EncryptionContext(secretKey, cipherDetails);
        final EncryptionState state = new EncryptionState(ctx);
        final byte[] iv = ctx.getCipher().getIV();

        // entity will always finalize encryption
        final EncryptingPartEntity.LastPartCallback callback = new EncryptingPartEntity.LastPartCallback() {
            @Override
            public ByteArrayOutputStream call(final long uploadedBytes) throws IOException {
                return state.remainderAndLastPartAuth();
            }
        };

        // prepare encryption state
        state.setMultipartStream(
                new MultipartOutputStream(
                        state.getEncryptionContext().getCipherDetails().getBlockSizeInBytes()));
        state.setCipherStream(
                EncryptingEntityHelper.makeCipherOutputForStream(
                        state.getMultipartStream(),
                        state.getEncryptionContext()));

        final int inputSize = RandomUtils.nextInt(300, 600);
        final byte[] content = RandomUtils.nextBytes(inputSize);
        final HttpEntity sourceEntity = new ExposedByteArrayEntity(content, ContentType.APPLICATION_OCTET_STREAM);

        final EncryptingPartEntity originalEntity = new EncryptingPartEntity(
                state.getCipherStream(),
                state.getMultipartStream(),
                sourceEntity,
                callback);

        final EncryptionStateSnapshot snapshot = EncryptionStateRecorder.record(state, null);

        final ByteArrayOutputStream originalOutput = new ByteArrayOutputStream();
        state.getLock().lock();
        originalEntity.writeTo(originalOutput);
        state.getLock().unlock();

        // rewind state so the next entity can use it
        EncryptionStateRecorder.rewind(state, snapshot);

        final EncryptingPartEntity retryEntity = new EncryptingPartEntity(
                state.getCipherStream(),
                state.getMultipartStream(),
                sourceEntity,
                callback);

        final ByteArrayOutputStream retryOutput = new ByteArrayOutputStream();
        state.getLock().lock();
        retryEntity.writeTo(retryOutput);
        state.getLock().unlock();

        // verify the parts encrypt to the same ciphertext
        final byte[] uploadedBytes = retryOutput.toByteArray();

        validateCiphertext(cipherDetails, secretKey, content, iv, uploadedBytes);

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