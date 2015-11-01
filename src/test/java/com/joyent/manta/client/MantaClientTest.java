/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.*;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.net.URL;
import java.util.Collection;
import java.util.UUID;


/**
 * @author Yunong Xiao
 */
public class MantaClientTest {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private MantaClient mantaClient;

    private String testDirPath;

    @BeforeClass
    @Parameters({"manta.url", "manta.accountName", "manta.test.key.private.filename", "manta.test.key.fingerprint"})
    public void beforeClass(@Optional("https://us-east.manta.joyent.com") String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId)
            throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        URL privateKeyUrl = mantaKeyPath == null ?
                null :
                Thread.currentThread().getContextClassLoader().getResource(mantaKeyPath);

        BaseChainedConfigContext testNgConfig = new StandardConfigContext()
                .setMantaURL(mantaUrl)
                .setMantaUser(mantaUser)
                .setMantaKeyId(mantaKeyId);

        if (privateKeyUrl != null) testNgConfig.setMantaKeyPath(privateKeyUrl.getFile());

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new ChainedConfigContext(
                // First read TestNG settings
                testNgConfig,
                // Then read Java system settings
                new SystemSettingsConfigContext(),
                // Then read environment variables
                new EnvVarConfigContext());

        mantaClient = MantaClient.newInstance(config);
        testDirPath = String.format("/%s/stor/%s/", config.getMantaUser(), UUID.randomUUID());
        mantaClient.putDirectory(testDirPath, null);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testDirPath);
        }
    }


    @Test
    public final void testCRUDObject() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaObject gotObject = mantaClient.get(testDirPath + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testDirPath + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testCRUDWithFileObject() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaObject gotObject = mantaClient.get(testDirPath + name);
        final File file = new File("/tmp/" + name);
        MantaUtils.inputStreamToFile(gotObject.getDataInputStream(), file);
        final String data = MantaUtils.readFileToString(file);
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testDirPath + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testCRUDObjectWithHeaders() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setHeader("durability-level", 4);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaObject gotObject = mantaClient.get(testDirPath + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        Assert.assertEquals(4, mantaObject.getHeader("durability-level"));
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testDirPath + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testRecursiveDeleteObject() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        mantaClient.deleteRecursive(mantaObject.getPath());

        boolean thrown = false;
        try {
            mantaClient.get(mantaObject.getPath());
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testPutWithStream() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        final InputStream testDataInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(TEST_FILENAME);
        mantaObject.setDataInputStream(testDataInputStream);
        mantaClient.put(mantaObject);
    }


    @Test
    public final void testHead() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaObject obj = mantaClient.head(mantaObject.getPath());
        Assert.assertNotNull(obj);
    }


    @Test
    public final void testPutLink() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = UUID.randomUUID().toString();
        final MantaObject original = new MantaObject(testDirPath + name);
        original.setDataInputString(TEST_DATA);
        mantaClient.put(original);

        final String link = UUID.randomUUID().toString();
        mantaClient.putSnapLink(testDirPath + link, testDirPath + name, null);
        final MantaObject linkObj = mantaClient.get(testDirPath + link);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(linkObj.getDataInputStream()));
        String data;
        while ((data = reader.readLine()) != null) {
            Assert.assertEquals(TEST_DATA, data);
        }
    }


    @Test
    public final void testList() throws IOException, MantaClientHttpResponseException, MantaCryptoException, MantaObjectException {
        final String pathPrefix = testDirPath + "/" + UUID.randomUUID().toString();
        mantaClient.putDirectory(pathPrefix, null);
        mantaClient.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()));
        mantaClient.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()));
        final String subDir = pathPrefix + "/" + UUID.randomUUID().toString();
        mantaClient.putDirectory(subDir, null);
        mantaClient.put(new MantaObject(subDir + "/" + UUID.randomUUID().toString()));
        final Collection<MantaObject> objs = mantaClient.listObjects(pathPrefix);
        for (final MantaObject mantaObject : objs) {
            Assert.assertTrue(mantaObject.getPath().startsWith(testDirPath));
        }
        Assert.assertEquals(3, objs.size());
    }


    @Test(expectedExceptions = MantaObjectException.class)
    public final void testListNotADir() throws IOException, MantaClientHttpResponseException,MantaCryptoException, MantaObjectException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        mantaClient.listObjects(mantaObject.getPath());
    }


    @Test
    public final void testRFC3986() throws IOException, MantaClientHttpResponseException, MantaCryptoException {
        final String name = "spaces in the name of the file";
        final MantaObject mantaObject = new MantaObject(testDirPath + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaObject gotObject = mantaClient.get(testDirPath + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testDirPath + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


}