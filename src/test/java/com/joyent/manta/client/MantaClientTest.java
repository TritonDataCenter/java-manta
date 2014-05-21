/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;

/**
 * @author Yunong Xiao
 */
public class MantaClientTest {

    private static MantaClient client;

    private static final String URL = "https://us-east.manta.joyent.com";
    private static final String LOGIN = "yunong";
    /** XXX: change this to a valid key when testing. */
    private static final String KEY_PATH = "src/test/java/data/id_rsa_test";
    private static final String KEY_FINGERPRINT = "2b:6d:a6:06:35:f7:a6:62:62:b4:5a:85:d7:58:6e:bb";
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILE = "src/test/java/data/Master-Yoda.jpg";
    private static final String TEST_DIR_PATH = "/yunong/stor/" + UUID.randomUUID().toString() + "/";

    @BeforeClass
    public static void beforeClass() throws IOException, MantaCryptoException, MantaClientHttpResponseException {
        client = MantaClient.newInstance(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);
        BasicConfigurator.configure();
        client.putDirectory(TEST_DIR_PATH, null);
    }

    @Test
    public final void testCRUDObject() throws MantaCryptoException, IOException, MantaClientHttpResponseException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        final MantaObject gotObject = client.get(TEST_DIR_PATH + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        assertEquals(mantaObject.getDataInputString(), data);
        client.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            client.get(TEST_DIR_PATH + name);
        } catch (final MantaClientHttpResponseException e) {
            assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test
    public final void testCRUDWithFileObject() throws MantaCryptoException, IOException,
    MantaClientHttpResponseException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        final MantaObject gotObject = client.get(TEST_DIR_PATH + name);
        final File file = new File("/tmp/" + name);
        MantaUtils.inputStreamToFile(gotObject.getDataInputStream(), file);
        final String data = FileUtils.readFileToString(file);
        assertEquals(mantaObject.getDataInputString(), data);
        client.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            client.get(TEST_DIR_PATH + name);
        } catch (final MantaClientHttpResponseException e) {
            assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test
    public final void testCRUDObjectWithHeaders() throws MantaCryptoException, IOException,
    MantaClientHttpResponseException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setHeader("durability-level", 4);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        final MantaObject gotObject = client.get(TEST_DIR_PATH + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        assertEquals(mantaObject.getDataInputString(), data);
        assertEquals(4, mantaObject.getHeader("durability-level"));
        client.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            client.get(TEST_DIR_PATH + name);
        } catch (final MantaClientHttpResponseException e) {
            assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test
    public final void testRecursiveDeleteObject() throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        client.deleteRecursive(mantaObject.getPath());

        boolean thrown = true;
        try {
            client.get(mantaObject.getPath());
        } catch (final MantaClientHttpResponseException e) {
            assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        assertTrue(thrown);
    }

    @Test
    public final void testPutWithStream() throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        final InputStream is = new FileInputStream(new File(TEST_FILE));
        mantaObject.setDataInputStream(is);
        client.put(mantaObject);
    }

    @Test
    public final void testHead() throws MantaCryptoException, IOException, MantaClientHttpResponseException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        final MantaObject obj = client.head(mantaObject.getPath());
        assertNotNull(obj);
    }

    @Test
    public final void testPutLink() throws MantaCryptoException, IOException, MantaClientHttpResponseException {
        final String name = UUID.randomUUID().toString();
        final MantaObject original = new MantaObject(TEST_DIR_PATH + name);
        original.setDataInputString(TEST_DATA);
        client.put(original);

        final String link = UUID.randomUUID().toString();
        client.putSnapLink(TEST_DIR_PATH + link, TEST_DIR_PATH + name, null);
        final MantaObject linkObj = client.get(TEST_DIR_PATH + link);
        final BufferedReader reader = new BufferedReader(new InputStreamReader(linkObj.getDataInputStream()));
        String data = null;
        while ((data = reader.readLine()) != null) {
            assertEquals(TEST_DATA, data);
        }
    }

    @Test
    public final void testList() throws MantaCryptoException, IOException, MantaObjectException,
    MantaClientHttpResponseException {
        final String pathPrefix = TEST_DIR_PATH + "/" + UUID.randomUUID().toString();
        client.putDirectory(pathPrefix, null);
        client.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()));
        client.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()));
        final String subDir = pathPrefix + "/" + UUID.randomUUID().toString();
        client.putDirectory(subDir, null);
        client.put(new MantaObject(subDir + "/" + UUID.randomUUID().toString()));
        final Collection<MantaObject> objs = client.listObjects(pathPrefix);
        for (final MantaObject mantaObject : objs) {
            assertTrue(mantaObject.getPath().startsWith(TEST_DIR_PATH));
        }
        assertEquals(3, objs.size());
    }

    @Test(expected = MantaObjectException.class)
    public final void testListNotADir() throws MantaCryptoException, MantaClientHttpResponseException, IOException,
    MantaObjectException {
        final String name = UUID.randomUUID().toString();
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        client.listObjects(mantaObject.getPath());
    }

    @Test
    public final void testRFC3986() throws MantaCryptoException, IOException, MantaClientHttpResponseException {
        final String name = "spaces in the name of the file";
        final MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
        mantaObject.setDataInputString(TEST_DATA);
        client.put(mantaObject);
        final MantaObject gotObject = client.get(TEST_DIR_PATH + name);
        final String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
        assertEquals(mantaObject.getDataInputString(), data);
        client.delete(mantaObject.getPath());
        boolean thrown = false;
        try {
            client.get(TEST_DIR_PATH + name);
        } catch (final MantaClientHttpResponseException e) {
            assertEquals(404, e.getStatusCode());
            thrown = true;
        }
        assertTrue(thrown);
    }

    @AfterClass
    public static void afterClass() throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        if (client != null) {
            client.deleteRecursive(TEST_DIR_PATH);
        }
    }
}
