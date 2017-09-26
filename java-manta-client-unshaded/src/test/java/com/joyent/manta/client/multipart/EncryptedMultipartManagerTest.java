/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.client.crypto.AesCtrCipherDetails;
import com.joyent.manta.client.crypto.EncryptionContext;
import com.joyent.manta.client.crypto.MantaEncryptedObjectInputStream;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedHmacsLookupMap;
import com.joyent.manta.config.BaseChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SettableConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.TestConfigContext;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.MantaHttpRequestFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Stream;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;

import static org.mockito.Mockito.mock;

@Test
public class EncryptedMultipartManagerTest {
    private SupportedCipherDetails cipherDetails = AesCtrCipherDetails.INSTANCE_128_BIT;
    private SecretKey secretKey = SecretKeyUtils.loadKey(
            Base64.getDecoder().decode("qAnCNUmmFjUTtImNGv241Q=="), cipherDetails);

    private TestMultipartManager testManager = new TestMultipartManager();
    private EncryptedMultipartManager<TestMultipartManager, TestMultipartUpload> manager;

    @BeforeClass
    public void setup() {
        this.manager = buildManager();
    }

    public void canDoMultipartUpload() throws Exception {
        MantaMetadata metadata = new MantaMetadata();
        metadata.put("m-my-key", "my value");
        metadata.put("e-my-key-1", "my value 1");
        metadata.put("e-my-key-2", "my value 2");

        MantaHttpHeaders headers = new MantaHttpHeaders();

        String path = "/user/stor/testobject";

        EncryptedMultipartUpload<TestMultipartUpload> upload =
                manager.initiateUpload(path, 35L, metadata, headers);

        ArrayList<String> lines = new ArrayList<>();
        lines.add("01234567890ABCDEF|}{");
        lines.add("ZYXWVUTSRQPONMLKJIHG");
        lines.add("!@#$%^&*()_+-=[]/,.<");
        lines.add(">~`?abcdefghijklmnop");
        lines.add("qrstuvxyz");

        String expected = StringUtils.join(lines, "");

        MantaMultipartUploadPart[] parts = new MantaMultipartUploadPart[5];

        for (int i = 0; i < lines.size(); i++) {
            parts[i] = manager.uploadPart(upload, i + 1, lines.get(i));
        }

        Stream<MantaMultipartUploadTuple> partsStream = Stream.of(parts);

        manager.complete(upload, partsStream);

        TestMultipartUpload actualUpload = upload.getWrapped();

        MantaMetadata actualMetadata = MantaObjectMapper.INSTANCE
                .readValue(actualUpload.getMetadata(), MantaMetadata.class);

        Assert.assertEquals(actualMetadata, metadata,
                "Metadata wasn't stored correctly");

        MantaHttpHeaders actualHeaders = MantaObjectMapper.INSTANCE
                .readValue(actualUpload.getHeaders(), MantaHttpHeaders.class);

        Assert.assertEquals(actualHeaders, headers,
                "Headers were stored correctly");

        {
            Cipher cipher = cipherDetails.getCipher();
            byte[] iv = upload.getEncryptionState().getEncryptionContext().getCipher().getIV();
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, cipherDetails.getEncryptionParameterSpec(iv));

            byte[] assumedCiphertext = cipher.doFinal(expected.getBytes(StandardCharsets.US_ASCII));
            byte[] contents = FileUtils.readFileToByteArray(upload.getWrapped().getContents());
            byte[] actualCiphertext = Arrays.copyOf(contents,
                    contents.length - cipherDetails.getAuthenticationTagOrHmacLengthInBytes());

            try {
                AssertJUnit.assertEquals(assumedCiphertext, actualCiphertext);
            } catch (AssertionError e) {
                System.err.println("expected: " + Hex.encodeHexString(assumedCiphertext));
                System.err.println("actual  : " + Hex.encodeHexString(actualCiphertext));
                throw e;
            }
        }

        EncryptionContext encryptionContext = upload.getEncryptionState().getEncryptionContext();

        MantaHttpHeaders responseHttpHeaders = new MantaHttpHeaders();
        responseHttpHeaders.setContentLength(actualUpload.getContents().length());

        final String hmacName = SupportedHmacsLookupMap.hmacNameFromInstance(
                encryptionContext.getCipherDetails().getAuthenticationHmac());

        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_HMAC_TYPE, hmacName);
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_IV,
                Base64.getEncoder().encodeToString(encryptionContext.getCipher().getIV()));
        responseHttpHeaders.put(MantaHttpHeaders.ENCRYPTION_KEY_ID,
                cipherDetails.getCipherId());

        MantaObjectResponse response = new MantaObjectResponse(path,
                responseHttpHeaders, metadata);


        CloseableHttpResponse httpResponse = mock(CloseableHttpResponse.class);

        try (InputStream fin = new FileInputStream(actualUpload.getContents());
             EofSensorInputStream eofIn = new EofSensorInputStream(fin, null);
             MantaObjectInputStream objIn = new MantaObjectInputStream(response, httpResponse, eofIn);
             InputStream in = new MantaEncryptedObjectInputStream(objIn, cipherDetails, secretKey, true)) {
            String actual = IOUtils.toString(in, StandardCharsets.UTF_8);

            Assert.assertEquals(actual, expected);
        }
    }

    // TEST UTILITY METHODS

    private SettableConfigContext<BaseChainedConfigContext> testConfigContext(SecretKey key) {
        StandardConfigContext settable = new StandardConfigContext();
        settable.setMantaUser("test");

        final String privateKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIEpQIBAAKCAQEA1lPONrT34W2VPlltA76E2JUX/8+Et7PiMiRNWAyrATLG7aRA\n" +
                "8iZ5A8o/aQMyexp+xgXoJIh18LmJ1iV8zqnr4TPXD2iPO92fyHWPu6P+qn0uw2Hu\n" +
                "ZZ0IvHHYED+fqxm7jz2ZjnfZl5Bz73ctjRF+77rPgOhhfv4KAc1d9CDsC+lHTqbp\n" +
                "ngufCYI4UWrnYoQ2JVXvEL9D5dMlHg0078qfh2cPg5xMOiOYobZeWqflV1Ue5I1Y\n" +
                "owNqiFzIDmBK0TKhnv+qQVNfMnNLJBYlYyGd0DUOJs8os5yivtuQXOhLZ0zLiTqK\n" +
                "JVjNJLzlcciqUf97Btm2enEHJ/khMFhrmoTQFQIDAQABAoIBAQCdc//grN4WHD0y\n" +
                "CtxNjd9mhVGWOsvTcTFRiN3RO609OiJuXubffmgU4rXm3dRuH67Wp2w9uop6iLO8\n" +
                "QNoJsUd6sGzkAvqHDm/eAo/PV9E1SrXaD83llJHgbvo+JZ+VQVhLCQQQZ/fQouyp\n" +
                "FbK/GgVY9LKQjydg9hw/6rGFMdJ3hFZVFqYFUhNpQKpczi6/lI/UIGcBhF3+8s/0\n" +
                "KMrz2PcCQFixlUFtBYXQHarOctxJDX7indchX08buwPqSv4YBBDLHUZkkMWomI/P\n" +
                "NjRDRyqnxvI03lHVfdbDzoPMxklJlHF68fkmp8NFLegnCBM8K0ae65Vk61b3oF9X\n" +
                "3eD6JtAZAoGBAPo/oBaJlA0GbQoJmULj6YqcQ2JKbUJtu7LP//8Gss47po4uqh6n\n" +
                "9vneKEpYYxuH5MXNsqtinmSQQMkE4UXoJSxJvnXNVAMQa3kUd0UgZSHjqWWgauDj\n" +
                "BjLQRpy9evef7VzTYx0xqEfAprsXxAoy0KXYN8gwgMC6MQgfZuFBgtxLAoGBANtA\n" +
                "1SVN/4wqrz4C8rpx7oZarHcMmGLiFF5OpKXlq1JY+U8IJ+WxMId3TI4h/h6OQGth\n" +
                "NJzQqFCS9H3a5EmqoNXHsLVXiKtG40+OzphSf9Y/NU7FtKanFWjfZl1ihhran1Fc\n" +
                "42jzN34EMM7Wm8p6HUK5qiDSCF+Ck0Lupud+WIkfAoGAXREOg3M0+UcbhDEfq23B\n" +
                "bAhDUymkyqCuvoh2hyzBkMtEXPpj0DTdN/3z8/o9GX8HiLzAJtbtWy7+uQO0l+AG\n" +
                "+xqN15e+F8mifowq8y1iDyFw3Ve0h+BGbN1idWZOdgsnJm+DG9dc4xp1p3zmLnjJ\n" +
                "efQYgr3vFD3qgD/Vbg6EEVMCgYEAnNfaIh+T6Y83YWL2hI2wFgiTS26FLGeSLoyP\n" +
                "l+WeEwB3CCRLdjK1BpM+/oYupWkZiDc3Td6uKUWXBNkrac9X0tZRAMinie7h+S2t\n" +
                "eKW7sWXyGnGv82+fDzCQp8ktKdSvF6MdQxyJ2+nfiHdZZxTIDc2HeIcHWlusQLs8\n" +
                "RmnJp/0CgYEA8AUV7K2KNRcwfuB1UjqhvlaqgiGixrItacGgnMQJ2cRSRSq2fZTm\n" +
                "eXxT9ugZ/9J9D4JTYZgdABnKvDjgbJMH9w8Nxr+kn/XZKNDzc1z0iJYwvyBOc1+e\n" +
                "prHvy4y+bCc0kLjCNQW4+/pVTWe1w8Mp63Vhdn+fO+wUGT3DTJGIXkU=\n" +
                "-----END RSA PRIVATE KEY-----";
        settable.setPrivateKeyContent(privateKey);

        final String fingerprint = "ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02";
        settable.setMantaKeyId(fingerprint);

        settable.setEncryptionPrivateKeyBytes(key.getEncoded());

        return new TestConfigContext(settable);
    }

    private EncryptedMultipartManager<TestMultipartManager, TestMultipartUpload> buildManager() {
        ConfigContext config = testConfigContext(secretKey);
        final KeyPair keyPair;
        try {
            keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        } catch (GeneralSecurityException e) {
            throw new AssertionError(e);
        }
        final ThreadLocalSigner signer = new ThreadLocalSigner(new Signer.Builder(keyPair));

        final MantaConnectionFactory connectionFactory = new MantaConnectionFactory(config, keyPair, signer);
        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);

        EncryptionHttpHelper httpHelper = new EncryptionHttpHelper(
                connectionContext,
                new MantaHttpRequestFactory(config),
                config);

        return new EncryptedMultipartManager<>(secretKey, cipherDetails,
                httpHelper, testManager);
    }
}
