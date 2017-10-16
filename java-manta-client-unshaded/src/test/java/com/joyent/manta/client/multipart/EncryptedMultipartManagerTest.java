/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

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
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.MantaHttpRequestFactory;
import com.joyent.manta.util.UnitTestConstants;
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
        settable.setPrivateKeyContent(UnitTestConstants.PRIVATE_KEY);
        settable.setMantaKeyId(UnitTestConstants.FINGERPRINT);

        settable.setEncryptionPrivateKeyBytes(key.getEncoded());

        return new TestConfigContext(settable);
    }

    private EncryptedMultipartManager<TestMultipartManager, TestMultipartUpload> buildManager() {
        ConfigContext config = testConfigContext(secretKey);

        final MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);

        EncryptionHttpHelper httpHelper = new EncryptionHttpHelper(
                connectionContext,
                new MantaHttpRequestFactory(UnitTestConstants.UNIT_TEST_URL),
                config);

        return new EncryptedMultipartManager<>(secretKey, cipherDetails,
                httpHelper, testManager);
    }
}
