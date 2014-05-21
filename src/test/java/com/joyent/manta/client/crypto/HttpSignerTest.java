/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.BasicConfigurator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.joyent.manta.exception.MantaCryptoException;

/**
 * @author Yunong Xiao
 */
public class HttpSignerTest {

    private static final String KEY_PATH = "src/test/java/data/id_rsa";
    private static final String KEY_FINGERPRINT = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";
    private static final String LOGIN = "yunong";
    private static final HttpRequestFactory REQUEST_FACTORY = new NetHttpTransport().createRequestFactory();
    private static HttpSigner httpSigner;
    /**
     * signer initialised from memory.
     */
    private static HttpSigner httpInMemeSigner;

    private static String readFile(final String path) throws IOException {
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

    @BeforeClass
    public static void beforeClass() throws IOException {
        httpSigner = HttpSigner.newInstance(KEY_PATH, KEY_FINGERPRINT, LOGIN);
        httpInMemeSigner = HttpSigner.newInstance(readFile(KEY_PATH), KEY_FINGERPRINT, null, LOGIN);
        BasicConfigurator.configure();
    }

    @Test
    public final void testSignDataWithInMemSigner() throws IOException, MantaCryptoException {
        final HttpRequest req = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        httpInMemeSigner.signRequest(req);
        final boolean verified = httpInMemeSigner.verifyRequest(req);
        Assert.assertTrue("unable to verify signed authorization header", verified);
    }

    @Test
    public final void testSignData() throws IOException, MantaCryptoException {
        final HttpRequest req = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        httpSigner.signRequest(req);
        final boolean verified = httpSigner.verifyRequest(req);
        Assert.assertTrue("unable to verify signed authorization header", verified);
    }
}
