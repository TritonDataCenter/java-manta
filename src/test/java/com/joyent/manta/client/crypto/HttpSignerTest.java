/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.crypto;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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

import static org.junit.Assert.assertNotNull;

/**
 * @author Yunong Xiao
 */
public class HttpSignerTest {

    private static final String KEY_PATH = "src/test/java/data/id_rsa";
    private static final String KEY_FINGERPRINT = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";
    private static final String LOGIN = "yunong";
    private static final HttpRequestFactory REQUEST_FACTORY = new NetHttpTransport().createRequestFactory();
    private static HttpSigner HTTP_SIGNER;
    private static HttpSigner HTTP_SIGNER_MEM; // signer initialised from memory

    private static String readFile(String path) {
        BufferedReader br = null;
        StringBuilder result = new StringBuilder();
        try {
            String line;
            br = new BufferedReader(new FileReader(KEY_PATH));
            while ((line = br.readLine()) != null) {
                result.append(line).append("\n");
            }
            return result.toString();
        } catch( FileNotFoundException e ) {
            e.printStackTrace();
        } catch( IOException e ) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try { br.close(); } catch( IOException e ) {}
            }
        }
        return null;
    }

    @BeforeClass
    public static void beforeClass() throws IOException {
        HTTP_SIGNER = HttpSigner.newInstance(KEY_PATH, KEY_FINGERPRINT, LOGIN);
        HTTP_SIGNER_MEM = HttpSigner.newInstance(readFile(KEY_PATH), KEY_FINGERPRINT, null, LOGIN);
        BasicConfigurator.configure();
    }

    @Test
    public void testSignDataWithInMemSigner() throws IOException, MantaCryptoException {
        HttpRequest req = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        HTTP_SIGNER_MEM.signRequest(req);
        boolean verified = HTTP_SIGNER_MEM.verifyRequest(req);
        Assert.assertTrue("unable to verify signed authorization header", verified);
    }

    @Test
    public void testSignData() throws IOException, MantaCryptoException {
        HttpRequest req = REQUEST_FACTORY.buildGetRequest(new GenericUrl());
        HTTP_SIGNER.signRequest(req);
        boolean verified = HTTP_SIGNER.verifyRequest(req);
        Assert.assertTrue("unable to verify signed authorization header", verified);
    }
}
