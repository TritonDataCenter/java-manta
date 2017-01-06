package com.joyent.manta.client.crypto;

import com.joyent.manta.http.entity.ExposedStringEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;

@Test
public class EncryptingEntityTest {
    public void canEncryptAndDecryptToAndFromFile() throws Exception {
        final Charset charset = Charsets.US_ASCII;
        final String expectedString = "012345678901245601234567890124";
        SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
        ExposedStringEntity stringEntity = new ExposedStringEntity(
                expectedString, charset);

        byte[] keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes(charset);
        SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

        EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                cipherDetails,
                stringEntity.getContentLength(), stringEntity);

        File file = File.createTempFile("ciphertext-", ".data");
        FileUtils.forceDeleteOnExit(file);

        try (FileOutputStream out = new FileOutputStream(file)) {
            encryptingEntity.writeTo(out);
        }

        Assert.assertEquals(file.length(), encryptingEntity.getContentLength());

        byte[] iv = encryptingEntity.getCipher().getIV();
        Cipher cipher = cipherDetails.getCipher();
        cipher.init(Cipher.DECRYPT_MODE, key, cipherDetails.getEncryptionParameterSpec(iv));

        try (FileInputStream in = new FileInputStream(file);
             CipherInputStream cin = new CipherInputStream(in, cipher)) {
            final byte[] actualBytes = IOUtils.toByteArray(cin);
            final String actual = new String(actualBytes, charset);
            Assert.assertEquals(actual, expectedString,
                    "Plaintext doesn't match decrypted value");
        }
    }
}
