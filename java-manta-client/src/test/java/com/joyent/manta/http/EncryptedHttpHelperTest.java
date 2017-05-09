/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.crypto.AesCbcCipherDetails;
import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaClientEncryptionException;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static com.joyent.manta.config.DefaultsConfigContext.DEFAULT_MANTA_URL;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


@Test
public class EncryptedHttpHelperTest {
    /**
     * Verifies that encrypted HEAD requests made with a different cipher/mode
     * configured from the cipher/mode on the object will result in an error.
     */
    @Test(groups = {"unlimited-crypto"})
    public void doesntAllowDifferentCipherForObjectAndConfigHeadIfOnlyReadingUnencryptedMetadata() throws Exception {
        String path = "/user/path/encrypted-object";
        EncryptionHttpHelper httpHelper = fakeEncryptionHttpHelper(path);
        httpHelper.httpHead(path);
    }

    /**
     * Verifies that encrypted GET requests made with a different cipher/mode
     * configured from the cipher/mode on the object will result in an error.
     */
    @Test(groups = {"unlimited-crypto"})
    public void allowDifferentCipherForObjectAndConfigGetIfOnlyReadingUnencryptedMetadata() throws Exception {
        String path = "/user/path/encrypted-object";
        EncryptionHttpHelper httpHelper = fakeEncryptionHttpHelper(path);
        httpHelper.httpGet(path);
    }

    /**
     * Verifies that encrypted requests that pass on an {@link java.io.InputStream}
     * made with a different cipher/mode configured from the cipher/mode on
     * the object will result in an error.
     */
    @Test(groups = {"unlimited-crypto"})
    public void doesntAllowDifferentCipherForObjectAndConfigInputStream() throws Exception {
        String path = "/user/path/encrypted-object";
        EncryptionHttpHelper httpHelper = fakeEncryptionHttpHelper(path);

        boolean caught = false;
        try {
            URI uri = URI.create(DEFAULT_MANTA_URL + "/" + path);
            HttpGet get = new HttpGet(uri);
            MantaHttpHeaders headers = new MantaHttpHeaders();
            httpHelper.httpRequestAsInputStream(get, headers);
        } catch (MantaClientEncryptionException e) {
            caught = e.getMessage().startsWith("Cipher used to encrypt object"
                    + " is not the same as the cipher configured");
        }

        Assert.assertTrue(caught, "No exception thrown when "
                + "configured cipher and object cipher differ");
    }

    /**
     * Builds a fully mocked {@link EncryptionHttpHelper} that is setup to
     * be configured for one cipher/mode and executes requests in another
     * cipher/mode.
     */
    private static EncryptionHttpHelper fakeEncryptionHttpHelper(String path)
            throws Exception {
        MantaConnectionContext connectionContext = mock(MantaConnectionContext.class);
        MantaConnectionFactory connectionFactory = mock(MantaConnectionFactory.class);
        StandardConfigContext config = new StandardConfigContext();

        SupportedCipherDetails cipherDetails = AesCbcCipherDetails.INSTANCE_192_BIT;

        config.setClientEncryptionEnabled(true)
                .setEncryptionPrivateKeyBytes(SecretKeyUtils.generate(cipherDetails).getEncoded())
                .setEncryptionAlgorithm(cipherDetails.getCipherId());

        EncryptionHttpHelper httpHelper = new EncryptionHttpHelper(
                connectionContext, connectionFactory, config);

        URI uri = URI.create(DEFAULT_MANTA_URL + "/" + path);

        when(connectionFactory.head(any())).thenReturn(new HttpHead(uri));
        when(connectionFactory.get(any())).thenReturn(new HttpGet(uri));

        CloseableHttpResponse fakeResponse = mock(CloseableHttpResponse.class);
        StatusLine statusLine = new BasicStatusLine(HttpVersion.HTTP_1_1,
                HttpStatus.SC_OK, "OK");


        SupportedCipherDetails objectCipherDetails =
                AesGcmCipherDetails.INSTANCE_128_BIT;

        Header[] headers = new Header[] {
                // Notice this is a different cipher than the one set in config
                new BasicHeader(MantaHttpHeaders.ENCRYPTION_CIPHER,
                        objectCipherDetails.getCipherId())
        };

        when(fakeResponse.getAllHeaders()).thenReturn(headers);
        when(fakeResponse.getStatusLine()).thenReturn(statusLine);

        BasicHttpEntity fakeEntity = new BasicHttpEntity();
        InputStream source = IOUtils.toInputStream("I'm a stream", StandardCharsets.US_ASCII);
        EofSensorInputStream stream = new EofSensorInputStream(source, null);
        fakeEntity.setContent(stream);
        when(fakeResponse.getEntity()).thenReturn(fakeEntity);

        when(connectionContext.getHttpClient()).thenReturn(new FakeCloseableHttpClient(fakeResponse));

        return httpHelper;
    }
}
