/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import org.apache.http.impl.cookie.DateParseException;
import org.apache.http.impl.cookie.DateUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;


/**
 * @author Yunong Xiao
 */
public class MantaClientTest {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private MantaClient mantaClient;

    private String testPathPrefix;


    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

        mantaClient = MantaClient.newInstance(config);
        testPathPrefix = String.format("/%s/stor/%s/", config.getMantaUser(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
        }
    }


    @Test
    public final void testCRUDObject() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);

        final MantaMetadata gotObject = mantaClient.get(testPathPrefix + name);
        Assert.assertNotNull(gotObject);
        Assert.assertNotNull(gotObject.getContentType());
        Assert.assertNotNull(gotObject.getContentLength());
        Assert.assertNotNull(gotObject.getEtag());
        Assert.assertNotNull(gotObject.getMtime());
        Assert.assertNotNull(gotObject.getPath());

        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testPathPrefix + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testCRUDWithFileObject() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaMetadata gotObject = mantaClient.get(testPathPrefix + name);
        final File file = new File("/tmp/" + name);
        MantaUtils.inputStreamToFile(gotObject.getDataInputStream(), file);
        final String data = MantaUtils.readFileToString(file);
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testPathPrefix + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testCRUDObjectWithHeaders() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        mantaObject.setHeader("durability-level", 4);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaMetadata gotObject = mantaClient.get(testPathPrefix + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        Assert.assertEquals(4, mantaObject.getHeader("durability-level"));
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testPathPrefix + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testDirectoryIsMarkedAsSuch() throws IOException {
        MantaObject dir = mantaClient.get(testPathPrefix);
        Assert.assertTrue(dir.isDirectory(),
                String.format("Directory should be marked as such [%s]", testPathPrefix));
    }


    @Test
    public final void testRecursiveDeleteObject() throws IOException {

        final MantaObject mantaObject = new MantaMetadata(testPathPrefix + UUID.randomUUID().toString());

        mantaClient.putDirectory(testPathPrefix + "1", null);
        final MantaMetadata mantaObject1 = new MantaMetadata(testPathPrefix + "1/"+ UUID.randomUUID().toString());
        mantaObject1.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject1);

        mantaClient.putDirectory(testPathPrefix + "1/2", null);
        final MantaMetadata mantaObject2 = new MantaMetadata(testPathPrefix + "1/2/" + UUID.randomUUID().toString());
        mantaObject2.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject2);

        mantaClient.putDirectory(testPathPrefix + "1/2/3", null);
        final MantaMetadata mantaObject3 = new MantaMetadata(testPathPrefix + "1/2/3/" + UUID.randomUUID().toString());
        mantaObject3.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject3);

        mantaClient.deleteRecursive(testPathPrefix + "1");

        boolean thrown = false;
        try {
            mantaClient.get(testPathPrefix + "1");
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test
    public final void testPutWithStream() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        final InputStream testDataInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(TEST_FILENAME);
        mantaObject.setDataInputStream(testDataInputStream);
        mantaClient.put(mantaObject);
    }


    @Test
    public final void testHead() throws IOException {
        final String objectName = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + objectName);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaMetadata mantaObjectHead = mantaClient.head(testPathPrefix + objectName);
        Assert.assertNotNull(mantaObjectHead);
        Assert.assertNotNull(mantaObjectHead.getContentType());
        Assert.assertNotNull(mantaObjectHead.getContentLength());
        Assert.assertNotNull(mantaObjectHead.getEtag());
        Assert.assertNotNull(mantaObjectHead.getMtime());
        Assert.assertNotNull(mantaObjectHead.getPath());

        final String directoryName = UUID.randomUUID().toString();
        mantaClient.putDirectory(testPathPrefix + directoryName, null);
        final MantaMetadata mantaDirectoryHead = mantaClient.head(testPathPrefix + directoryName);
        Assert.assertNotNull(mantaDirectoryHead);
        Assert.assertNotNull(mantaDirectoryHead.getContentType());
        Assert.assertNull(mantaDirectoryHead.getContentLength());
        Assert.assertNull(mantaDirectoryHead.getEtag());
        Assert.assertNotNull(mantaDirectoryHead.getMtime());
        Assert.assertNotNull(mantaDirectoryHead.getPath());

        final String linkName = UUID.randomUUID().toString();
        mantaClient.putSnapLink(testPathPrefix + linkName, testPathPrefix + objectName, null);
        final MantaMetadata mantaLinkHead = mantaClient.head(testPathPrefix + linkName);
        Assert.assertNotNull(mantaLinkHead);
        Assert.assertNotNull(mantaLinkHead.getContentType());
        Assert.assertNotNull(mantaLinkHead.getContentLength());
        Assert.assertNotNull(mantaLinkHead.getEtag());
        Assert.assertNotNull(mantaLinkHead.getMtime());
        Assert.assertNotNull(mantaLinkHead.getPath());

        Assert.assertEquals(mantaObjectHead.getContentType(), mantaLinkHead.getContentType());
        Assert.assertEquals(mantaObjectHead.getContentLength(), mantaLinkHead.getContentLength());
        Assert.assertEquals(mantaObjectHead.getEtag(), mantaLinkHead.getEtag());
    }


    @Test
    public final void testPutLink() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata original = new MantaMetadata(testPathPrefix + name);
        original.setDataInputString(TEST_DATA);
        mantaClient.put(original);

        final String link = UUID.randomUUID().toString();
        mantaClient.putSnapLink(testPathPrefix + link, testPathPrefix + name, null);
        final MantaMetadata linkObj = mantaClient.get(testPathPrefix + link);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(linkObj.getDataInputStream()));
        String data;
        while ((data = reader.readLine()) != null) {
            Assert.assertEquals(TEST_DATA, data);
        }
    }


    @Test
    public final void testList() throws IOException {
        final String pathPrefix = testPathPrefix + "/" + UUID.randomUUID().toString();
        mantaClient.putDirectory(pathPrefix, null);
        mantaClient.put(new MantaMetadata(pathPrefix + "/" + UUID.randomUUID().toString()));
        mantaClient.put(new MantaMetadata(pathPrefix + "/" + UUID.randomUUID().toString()));
        final String subDir = pathPrefix + "/" + UUID.randomUUID().toString();
        mantaClient.putDirectory(subDir, null);
        mantaClient.put(new MantaMetadata(subDir + "/" + UUID.randomUUID().toString()));
        final Collection<MantaObject> objs = mantaClient.listObjects(pathPrefix);
        for (final MantaObject mantaObject : objs) {
            Assert.assertTrue(mantaObject.getPath().startsWith(testPathPrefix));
        }
        Assert.assertEquals(3, objs.size());
    }


    @Test(expectedExceptions = MantaObjectException.class)
    public final void testListNotADir() throws IOException {
        final String name = UUID.randomUUID().toString();
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        mantaClient.listObjects(mantaObject.getPath());
    }


    @Test
    public final void testRFC3986() throws IOException {
        final String name = "spaces in the name of the file";
        final MantaMetadata mantaObject = new MantaMetadata(testPathPrefix + name);
        mantaObject.setDataInputString(TEST_DATA);
        mantaClient.put(mantaObject);
        final MantaMetadata gotObject = mantaClient.get(testPathPrefix + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        Assert.assertEquals(mantaObject.getDataInputString(), data);
        mantaClient.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            mantaClient.get(testPathPrefix + name);
        } catch (final MantaClientHttpResponseException e) {
            Assert.assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        Assert.assertTrue(thrown);
    }


    @Test(groups = { "mtime" })
    public final void testGetLastModifiedDate() throws DateParseException {
        final String mtime = "Wed, 11 Nov 2015 18:20:20 GMT";
        final Date expected = DateUtils.parseDate(mtime);
        final MantaMetadata obj = new MantaMetadata(testPathPrefix);
        obj.setMtime(mtime);

        Assert.assertEquals(obj.getLastModifiedTime(), expected,
                "Last modified date should equal input to mtime");
    }


    @Test(groups = { "mtime" })
    public final void testGetNullLastModifiedDate() throws DateParseException {
        final MantaMetadata obj = new MantaMetadata(testPathPrefix);
        obj.setMtime(null);

        Assert.assertNull(obj.getLastModifiedTime(),
                "Last modified date should be null when mtime is null");
    }


    @Test(groups = { "mtime" })
    public final void testGetLastModifiedDateWithUnparseableMtime() throws DateParseException {
        final String mtime = "Bad unparseable string";
        final MantaMetadata obj = new MantaMetadata(testPathPrefix);
        obj.setMtime(mtime);

        Assert.assertNull(obj.getLastModifiedTime(),
                "Last modified date should be null when mtime is null");
    }


    @Test(groups = { "mtime" })
    public final void testSetLastModifiedDate() throws DateParseException {
        final String mtime = "Wed, 11 Nov 2015 18:20:20 GMT";
        final Date input = DateUtils.parseDate(mtime);
        final MantaMetadata obj = new MantaMetadata(testPathPrefix);

        obj.setLastModifiedTime(input);

        Assert.assertEquals(obj.getLastModifiedTime(), input,
                "Last modified date should equal input to mtime");
        Assert.assertEquals(obj.getMtime(), mtime,
                "Mtime should equal input to Last modified date");

    }
}
