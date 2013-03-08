/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
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

import com.google.api.client.http.HttpHeaders;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.api.client.http.HttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;

/**
 * @author Yunong Xiao
 */
public class MantaClientTest {

        private static MantaClient CLIENT;

        private static final String URL = "https://manta-beta.joyentcloud.com";
        private static final String LOGIN = "yunong";
        private static final String KEY_PATH = "src/test/java/data/id_rsa";
        private static final String KEY_FINGERPRINT = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";
        private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
        private static final String TEST_FILE = "src/test/java/data/Master-Yoda.jpg";
        private static final String TEST_DIR_PATH = "/yunong/stor/" + UUID.randomUUID().toString() + "/";

        @BeforeClass
        public static void beforeClass() throws IOException, MantaCryptoException {
                CLIENT = MantaClient.newInstance(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);
                BasicConfigurator.configure();
                CLIENT.putDirectory(TEST_DIR_PATH, null);
        }

        @Test
        public void testCRUDObject() throws MantaCryptoException, IOException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                mantaObject.setDataInputString(TEST_DATA);
                CLIENT.put(mantaObject, null);
                MantaObject gotObject = CLIENT.get(TEST_DIR_PATH + name);
                String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
                assertEquals(mantaObject.getDataInputString(), data);
                CLIENT.delete(mantaObject.getPath());
                boolean thrown = false;
                try {
                        CLIENT.get(TEST_DIR_PATH + name);
                } catch (HttpResponseException e) {
                        assertEquals(404, e.getStatusCode());
                        thrown = true;
                }
                assertTrue(thrown);
        }

        @Test
        public void testCRUDWithFileObject() throws MantaCryptoException, IOException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                mantaObject.setDataInputString(TEST_DATA);
                CLIENT.put(mantaObject, null);
                MantaObject gotObject = CLIENT.get(TEST_DIR_PATH + name);
                File file = new File("/tmp/" + name);
                MantaUtils.inputStreamToFile(gotObject.getDataInputStream(), file);
                String data = FileUtils.readFileToString(file);
                assertEquals(mantaObject.getDataInputString(), data);
                CLIENT.delete(mantaObject.getPath());
                boolean thrown = false;
                try {
                        CLIENT.get(TEST_DIR_PATH + name);
                } catch (HttpResponseException e) {
                        assertEquals(404, e.getStatusCode());
                        thrown = true;
                }
                assertTrue(thrown);
        }

        @Test
        public void testRecursiveDeleteObject() throws MantaCryptoException, HttpResponseException, IOException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                mantaObject.setDataInputString(TEST_DATA);
                CLIENT.put(mantaObject, null);
                CLIENT.deleteRecursive(mantaObject.getPath());

                boolean thrown = true;
                try {
                        CLIENT.get(mantaObject.getPath());
                } catch (HttpResponseException e) {
                        assertEquals(404, e.getStatusCode());
                        thrown = true;
                }
                assertTrue(thrown);
        }

        @Test
        public void testPutWithStream() throws MantaCryptoException, HttpResponseException, IOException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                InputStream is = new FileInputStream(new File(TEST_FILE));
                mantaObject.setDataInputStream(is);
                CLIENT.put(mantaObject, null);
        }

        @Test
        public void testPutWithDurability() throws MantaCryptoException, HttpResponseException, IOException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                String testPath = TEST_DIR_PATH + name;
                MantaObject mantaObject = new MantaObject(testPath);
                InputStream is = new FileInputStream(new File(TEST_FILE));
                mantaObject.setDataInputStream(is);
                mantaObject.setDurabilityLevel(3);
                CLIENT.put(mantaObject, null);
                MantaObject obj = CLIENT.get(testPath);
                assertEquals((Integer) 3, obj.getDurabilityLevel());
        }

        @Test
        public void testPutWithDurabilityHeader() throws MantaCryptoException, HttpResponseException, IOException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                String testPath = TEST_DIR_PATH + name;
                MantaObject mantaObject = new MantaObject(testPath);
                InputStream is = new FileInputStream(new File(TEST_FILE));
                mantaObject.setDataInputStream(is);
                mantaObject.setDurabilityLevel(4);
                HttpHeaders headers = new HttpHeaders();
                headers.put(MantaObject.DURABILITY_LEVEL, 3);
                CLIENT.put(mantaObject, headers);
                MantaObject obj = CLIENT.get(testPath);
                assertEquals((Integer) 3, obj.getDurabilityLevel());
        }

        @Test
        public void testHead() throws MantaCryptoException, IOException, HttpResponseException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                mantaObject.setDataInputString(TEST_DATA);
                CLIENT.put(mantaObject, null);
                MantaObject obj = CLIENT.head(mantaObject.getPath());
                assertNotNull(obj);
        }

        @Test
        public void testPutLink() throws MantaCryptoException, IOException, HttpResponseException, MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject original = new MantaObject(TEST_DIR_PATH + name);
                original.setDataInputString(TEST_DATA);
                CLIENT.put(original, null);

                String link = UUID.randomUUID().toString();
                CLIENT.putSnapLink(TEST_DIR_PATH + link, TEST_DIR_PATH + name, null);
                MantaObject linkObj = CLIENT.get(TEST_DIR_PATH + link);
                BufferedReader reader = new BufferedReader(new InputStreamReader(linkObj.getDataInputStream()));
                String data = null;
                while ((data = reader.readLine()) != null) {
                        assertEquals(TEST_DATA, data);
                }
        }

        @Test
        public void testList() throws MantaCryptoException, IOException, MantaObjectException {
                String pathPrefix = TEST_DIR_PATH + "/" + UUID.randomUUID().toString();
                CLIENT.putDirectory(pathPrefix, null);
                CLIENT.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()), null);
                CLIENT.put(new MantaObject(pathPrefix + "/" + UUID.randomUUID().toString()), null);
                String subDir = pathPrefix + "/" + UUID.randomUUID().toString();
                CLIENT.putDirectory(subDir, null);
                CLIENT.put(new MantaObject(subDir + "/" + UUID.randomUUID().toString()), null);
                Collection<MantaObject> objs = CLIENT.listObjects(pathPrefix);
                for (MantaObject mantaObject : objs) {
                        assertTrue(mantaObject.getPath().startsWith(TEST_DIR_PATH));
                }
                assertEquals(3, objs.size());
        }

        @Test(expected = MantaObjectException.class)
        public void testListNotADir() throws MantaCryptoException, HttpResponseException, IOException,
                        MantaObjectException {
                String name = UUID.randomUUID().toString();
                MantaObject mantaObject = new MantaObject(TEST_DIR_PATH + name);
                mantaObject.setDataInputString(TEST_DATA);
                CLIENT.put(mantaObject, null);
                CLIENT.listObjects(mantaObject.getPath());
        }

        @AfterClass
        public static void afterClass() throws MantaCryptoException, HttpResponseException, IOException {
                CLIENT.deleteRecursive(TEST_DIR_PATH);
        }
}