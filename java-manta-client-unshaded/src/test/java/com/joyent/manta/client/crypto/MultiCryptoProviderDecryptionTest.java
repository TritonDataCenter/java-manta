/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.client.MantaObjectResponse;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.Validate;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.conn.EofSensorInputStream;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.*;
import org.testng.annotations.Optional;

import static com.joyent.manta.client.crypto.ExternalSecurityProviderLoader.*;
import static com.joyent.manta.http.MantaHttpHeaders.*;
import static org.apache.http.HttpHeaders.*;
import static org.apache.commons.codec.binary.Hex.decodeHex;

import javax.crypto.SecretKey;
import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Provider;
import java.util.*;

/**
 * This test verifies that each of the supported ciphers and modes can be used to decrypt
 * a ciphertext blob when using different cryptographic providers. Furthermore, as the
 * cryptographic providers are upgraded, this test will verify that decryption works for
 * ciphertext encrypted using an older version of a cryptographic provider.
 * Note: Ensure before running this Test Class that you invoke the test configuration
 * specified in the testng.xml file.
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 * @since 3.3.0
 */
@Test(groups={"encryption-provider"})
public class MultiCryptoProviderDecryptionTest {
    private static final Logger LOG = LoggerFactory.getLogger(MultiCryptoProviderDecryptionTest.class);
    private static final List<SupportedCipherDetails> CIPHERS_TO_TEST = Collections.unmodifiableList(Arrays.asList(
                AesGcmCipherDetails.INSTANCE_128_BIT,
                AesGcmCipherDetails.INSTANCE_192_BIT,
                AesGcmCipherDetails.INSTANCE_256_BIT,
                AesCtrCipherDetails.INSTANCE_128_BIT,
                AesCtrCipherDetails.INSTANCE_192_BIT,
                AesCtrCipherDetails.INSTANCE_256_BIT,
                AesCbcCipherDetails.INSTANCE_128_BIT,
                AesCbcCipherDetails.INSTANCE_192_BIT,
                AesCbcCipherDetails.INSTANCE_256_BIT));

    private byte[] expectedContent;

    @BeforeClass()
    @Parameters({"preferredProviders"})
    public void beforeClass(@Optional final String preferredProviders) throws IOException {
        /* Warn user to invoke it from the TestNG suite */
        Validate.notNull(preferredProviders,
                    "Invoke value of parameter preferredProviders from the TestNG Suite!");

        /* Backup provider list from static context */
        List<Provider> providers = buildRankedPreferredProviders(preferredProviders);
        setRankedPreferredProviders(providers);
        final URL testURL = getClass().getClassLoader().getResource("test-data/chaucer.txt");

        try (InputStream in = testURL.openStream()) {
            expectedContent = IOUtils.toByteArray(in);
        } catch (NullPointerException e) {
            LOG.warn("Original Data from resources for" +
                    "Unit Test wasn't loaded properly", e);
            throw e;
        }
    }

    public void validateAllSupportedCiphers() throws IOException, DecoderException {
        /* Loop through each Supported Cipher for All Supported Providers*/
        for (final SupportedCipherDetails cipherDetails : CIPHERS_TO_TEST) {
            validateEncryptedFileWithHeaderValues(cipherDetails);
        }
    }

    /* Test Utility methods used in all unit-tests. */

    private void validateEncryptedFileWithHeaderValues(final SupportedCipherDetails cipherDetails) throws IOException, DecoderException {
        final String cipherName = cipherDetails.getCipherId().replaceAll("/","-");
        final SecretKey secretKey = generateSecretKey(cipherDetails);
        final String filePath = "test-data/ciphertext/" + cipherName + ".ciphertext";
        final URL encryptedFileURL = getClass().getClassLoader().getResource(filePath);

        MantaHttpHeaders getResponseHttpHeaders = new MantaHttpHeaders();
        populateHeaderValuesFromResources(getResponseHttpHeaders,cipherDetails);
        final long cipherTextSize = getResponseHttpHeaders.getContentLength();
        final byte[] actualBytes;

        try (final InputStream encryptedStream = encryptedFileURL.openStream();
             final BoundedInputStream bin = new BoundedInputStream(encryptedStream, cipherTextSize)) {
            actualBytes = IOUtils.toByteArray(bin);
        }

        final MantaObjectResponse objectResponse = new MantaObjectResponse(filePath, getResponseHttpHeaders);

        try (final InputStream sourceBytes = new ByteArrayInputStream(actualBytes);
             final EofSensorInputStream eofSensorInputStream = new EofSensorInputStream(sourceBytes,null);
             final MantaObjectInputStream objectStream = new MantaObjectInputStream(
                     objectResponse, Mockito.mock(CloseableHttpResponse.class), eofSensorInputStream);
             final MantaEncryptedObjectInputStream decryptingStream = new MantaEncryptedObjectInputStream(
                     objectStream, cipherDetails, secretKey, true);
             final ByteArrayOutputStream decrypted = new ByteArrayOutputStream()) {

            IOUtils.copy(decryptingStream, decrypted);
            validateDecryptingByteArrayForCipher(expectedContent, decrypted.toByteArray(), cipherName);
        }
    }

    private Map<String, Object> parseHeaderFileInfoStoreInMap(final URL headerURL) throws IOException {
        final Map<String, Object> headerMap = new CaseInsensitiveMap<>();

        /* Storing headers in a HashMap temporarily. */

        try (InputStream in = headerURL.openStream();
             final Scanner scanner = new Scanner(in, StandardCharsets.UTF_8.name())) {

            while (scanner.hasNextLine()) {
                String currLine = scanner.nextLine();
                if (currLine.startsWith("m-encrypt") || currLine.startsWith("content")) {
                    final String[] fields = currLine.split(":", 2);
                    if (fields.length == 2) {
                        headerMap.put(fields[0].trim(), fields[1].trim());
                    }
                    else {
                        LOG.error("Header Field Values parsed from the file are invalid!");
                    }
                }
            }
        }
        return headerMap;
    }

    private void populateHeaderValuesFromResources(final MantaHttpHeaders getResponseHttpHeaders,
                                                   final SupportedCipherDetails cipherDetails) throws IOException {
        final String cipherName = cipherDetails.getCipherId().replaceAll("/","-");
        final URL headerURL = getClass().getClassLoader().getResource("test-data/ciphertext"+
                MantaClient.SEPARATOR + cipherName + ".headers");
        final Map<String, Object> headerMap = parseHeaderFileInfoStoreInMap(headerURL);

        /* Populate header values */

        if (!cipherDetails.isAEADCipher()) {
            getResponseHttpHeaders.put(ENCRYPTION_HMAC_TYPE,
                    headerMap.get(ENCRYPTION_HMAC_TYPE));

            getResponseHttpHeaders.put(ENCRYPTION_METADATA_HMAC,
                    headerMap.get(ENCRYPTION_HMAC_TYPE));
        }
        else {
            getResponseHttpHeaders.put(ENCRYPTION_METADATA_AEAD_TAG_LENGTH,
                    headerMap.get(ENCRYPTION_METADATA_AEAD_TAG_LENGTH));
        }

        final long contentLength = Long.parseLong(headerMap.get(CONTENT_LENGTH).toString());
        getResponseHttpHeaders.setContentLength(contentLength);

        getResponseHttpHeaders.put(ENCRYPTION_IV,
                headerMap.get(ENCRYPTION_IV));

        getResponseHttpHeaders.put(ENCRYPTION_METADATA_IV,
                headerMap.get(ENCRYPTION_METADATA_IV));

        getResponseHttpHeaders.put(ENCRYPTION_KEY_ID,
                headerMap.get(ENCRYPTION_KEY_ID));

        getResponseHttpHeaders.put(ENCRYPTION_CIPHER,
                headerMap.get(ENCRYPTION_CIPHER));

        getResponseHttpHeaders.put(ENCRYPTION_PLAINTEXT_CONTENT_LENGTH,
                headerMap.get(ENCRYPTION_PLAINTEXT_CONTENT_LENGTH));

        getResponseHttpHeaders.put(ENCRYPTION_AEAD_TAG_LENGTH,
                headerMap.get(ENCRYPTION_AEAD_TAG_LENGTH));

        getResponseHttpHeaders.put(COMPUTED_MD5,
                headerMap.get(ENCRYPTION_METADATA_AEAD_TAG_LENGTH));

        getResponseHttpHeaders.put(ENCRYPTION_METADATA_AEAD_TAG_LENGTH,
                headerMap.get(ENCRYPTION_METADATA_AEAD_TAG_LENGTH));

        getResponseHttpHeaders.put(ENCRYPTION_TYPE,
                headerMap.get(ENCRYPTION_TYPE));

        getResponseHttpHeaders.setContentMD5(headerMap.get(CONTENT_MD5).toString());
        getResponseHttpHeaders.setContentType(headerMap.get(CONTENT_TYPE).toString());
    }

    private static SecretKey generateSecretKey(final SupportedCipherDetails cipherDetails) throws DecoderException {
        final byte[] secretKey = decodeHex((
                "8b30335ba65d1d0619c6192edb15318763d9a1be3ff916aaf46f4717232a504a").toCharArray());
        final int keySizeInBytes = cipherDetails.getKeyLengthBits() >> 3; // convert bits to bytes
        final byte[] secret = Arrays.copyOfRange(secretKey, 0, keySizeInBytes);
        return SecretKeyUtils.loadKey(secret, cipherDetails);
    }

    private static void validateDecryptingByteArrayForCipher(final byte[] actual, final byte[] expected, final String cipherName) {
        if (!Arrays.equals(actual, expected)) {
            final String msg = String.format("Validation of decryption for given "
                    + "Supported Cipher failed: %s\n", cipherName);

            Assert.fail(msg);
        }
    }
}