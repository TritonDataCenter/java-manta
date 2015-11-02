/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;


/**
 * @author Yunong Xiao
 */
public class HttpSignerTest {

    private static final HttpRequestFactory REQUEST_FACTORY = new NetHttpTransport().createRequestFactory();

    private HttpSigner httpSigner;

    private HttpSigner httpSignerInitializedWithInMemoryKeyData;


    @BeforeClass
    @Parameters({"manta.key_path", "manta.key_id", "manta.user"})
    public void beforeClass(@Optional String privateKeyFilename,
                            @Optional String keyFingerPrint,
                            @Optional String accountName)
            throws IOException, NoSuchAlgorithmException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                null, accountName, privateKeyFilename, keyFingerPrint, null);

        httpSigner = HttpSigner.newInstance(config.getMantaKeyPath(),
                config.getMantaKeyId(), config.getMantaUser());
        String privateKeyContent = readFile(config.getMantaKeyPath());
        httpSignerInitializedWithInMemoryKeyData = HttpSigner.newInstance(
                privateKeyContent, config.getMantaKeyId(), null, config.getMantaUser());
    }


    @Test
    public final void testSignDataWithInMemSigner() throws IOException, MantaCryptoException {
        final HttpRequest httpRequest = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        httpSignerInitializedWithInMemoryKeyData.signRequest(httpRequest);
        final boolean verified = httpSignerInitializedWithInMemoryKeyData.verifyRequest(httpRequest);
        Assert.assertTrue(verified, "unable to verify signed authorization header");
    }


    @Test
    public final void testSignData() throws IOException, MantaCryptoException {
        final HttpRequest httpRequest = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        httpSigner.signRequest(httpRequest);
        final boolean verified = httpSigner.verifyRequest(httpRequest);
        Assert.assertTrue(verified, "unable to verify signed authorization header");
    }


    private String readFile(final String path) throws IOException {
        BufferedReader br = null;
        final StringBuilder result = new StringBuilder();
        try {
            String line;
            br = new BufferedReader(new FileReader(path));
            while ((line = br.readLine()) != null) {
                result.append(line).append("\n");
            }
            return result.toString();
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }


}
