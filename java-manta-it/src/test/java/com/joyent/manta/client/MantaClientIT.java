/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.apache.http.client.utils.DateUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;


/**
 * Tests the basic functionality of the {@link MantaClient} class.
 *
 * @author <a href="https://github.com/yunong">Yunong Xiao</a>
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
//@Test(dependsOnGroups = { "directory" })
public class MantaClientIT {

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";
    private static final String TEST_FILENAME = "Master-Yoda.jpg";

    private MantaClient mantaClient;

    private String testPathPrefix;


    @BeforeClass()
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout", "manta.http_transport"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout,
                            @Optional String mantaHttpTransport)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout,
                mantaHttpTransport);

        mantaClient = new MantaClient(config);
        testPathPrefix = String.format("%s/stor/%s/",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix);
    }


    @AfterClass
    public void afterClass() throws IOException, MantaCryptoException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }


    @Test
    public final void testCRUDObject() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);

        try (final MantaObjectInputStream gotObject = mantaClient.getAsInputStream(path)) {
            Assert.assertNotNull(gotObject);
            Assert.assertNotNull(gotObject.getContentType());
            Assert.assertNotNull(gotObject.getContentLength());
            Assert.assertNotNull(gotObject.getEtag());
            Assert.assertNotNull(gotObject.getMtime());
            Assert.assertNotNull(gotObject.getPath());

            final String data = MantaUtils.inputStreamToString(gotObject);
            Assert.assertEquals(data, TEST_DATA);
        }

        mantaClient.delete(path);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + name));
    }


    @Test()
    public final void testManyOperations() throws IOException {
        String dir = String.format("%s/multiple", testPathPrefix);
        mantaClient.putDirectory(dir);

        for (int i = 0; i < 100; i++) {
            final String name = UUID.randomUUID().toString();
            final String path = String.format("%s/%s", dir, name);
            mantaClient.put(path, TEST_DATA);
            String actual = mantaClient.getAsString(path);
            Assert.assertEquals(actual, TEST_DATA);
        }

        mantaClient.deleteRecursive(dir);
    }


    @Test
    public final void testCRUDWithFileObject() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);
        final File file = mantaClient.getToTempFile(path);

        final String data = MantaUtils.readFileToString(file);
        Assert.assertEquals(data, TEST_DATA);
        mantaClient.delete(path);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + name));
    }


    @Test
    public final void testCRUDObjectWithHeaders() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.set("durability-level", 4);

        mantaClient.put(path, TEST_DATA, headers);
        try (final MantaObjectInputStream gotObject = mantaClient.getAsInputStream(path)) {
            final String data = MantaUtils.inputStreamToString(gotObject);
            Assert.assertEquals(data, TEST_DATA);
            Assert.assertEquals("4", gotObject.getHttpHeaders().getFirstHeaderStringValue("durability-level"));
            mantaClient.delete(gotObject.getPath());
        }

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + name));
    }


    @Test
    public final void testRecursiveDeleteObject() throws IOException {
        final String dir1 = String.format("%s1", testPathPrefix);
                mantaClient.putDirectory(testPathPrefix + "1", null);
        mantaClient.putDirectory(dir1, null);
        final String path1 = String.format("%s/%s", dir1, UUID.randomUUID());
        mantaClient.put(path1, TEST_DATA);

        final String dir2 = String.format("%s/2", dir1);
        mantaClient.putDirectory(dir2, null);
        final String path2 = String.format("%s/%s", dir2, UUID.randomUUID());
        mantaClient.put(path2, TEST_DATA);

        final String dir3 = String.format("%s/3", dir2);
        mantaClient.putDirectory(dir3, null);
        final String path3 = String.format("%s/%s", dir3, UUID.randomUUID());
        mantaClient.put(path3, TEST_DATA);

        mantaClient.deleteRecursive(testPathPrefix + "1");

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + "1"));
    }


    @Test
    public final void testPutWithStream() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Assert.assertNotNull(classLoader.getResource(TEST_FILENAME));

        try (InputStream testDataInputStream = classLoader.getResourceAsStream(TEST_FILENAME)) {
            mantaClient.put(path, testDataInputStream);
        }
    }


    @Test
    public final void testHead() throws IOException {
        final String objectName = UUID.randomUUID().toString();
        final String path = testPathPrefix + objectName;

        mantaClient.put(path, TEST_DATA);
        final MantaObjectResponse mantaObjectHead = mantaClient.head(testPathPrefix + objectName);
        Assert.assertNotNull(mantaObjectHead);
        Assert.assertNotNull(mantaObjectHead.getContentType());
        Assert.assertNotNull(mantaObjectHead.getContentLength());
        Assert.assertNotNull(mantaObjectHead.getEtag());
        Assert.assertNotNull(mantaObjectHead.getMtime());
        Assert.assertNotNull(mantaObjectHead.getPath());

        final String directoryName = UUID.randomUUID().toString();
        mantaClient.putDirectory(testPathPrefix + directoryName, null);
        final MantaObjectResponse mantaDirectoryHead = mantaClient.head(testPathPrefix + directoryName);
        Assert.assertNotNull(mantaDirectoryHead);
        Assert.assertNotNull(mantaDirectoryHead.getContentType());
        Assert.assertNull(mantaDirectoryHead.getContentLength());
        Assert.assertNull(mantaDirectoryHead.getEtag());
        Assert.assertNotNull(mantaDirectoryHead.getMtime());
        Assert.assertNotNull(mantaDirectoryHead.getPath());

        final String linkName = UUID.randomUUID().toString();
        mantaClient.putSnapLink(testPathPrefix + linkName, testPathPrefix + objectName, null);
        final MantaObjectResponse mantaLinkHead = mantaClient.head(testPathPrefix + linkName);
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
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);

        final String link = UUID.randomUUID().toString();
        mantaClient.putSnapLink(testPathPrefix + link, testPathPrefix + name, null);
        final String linkContent = mantaClient.getAsString(testPathPrefix + link);
        Assert.assertEquals(linkContent, TEST_DATA);

    }


    @Test
    public final void canMoveFile() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + "/";
        mantaClient.putDirectory(newDir);
        final String newPath = newDir + "this-is-a-new-name.txt";

        mantaClient.move(path, newPath);
        final String movedContent = mantaClient.getAsString(newPath);
        Assert.assertEquals(movedContent, TEST_DATA);
    }


    @Test
    public final void canMoveEmptyDirectory() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.putDirectory(path);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + "/";

        mantaClient.move(path, newDir);
        boolean newLocationExists = mantaClient.existsAndIsAccessible(newDir);
        Assert.assertTrue(newLocationExists, "Destination directory doesn't exist: "
                + newDir);

        MantaObjectResponse head = mantaClient.head(newDir);

        boolean isDirectory = head.isDirectory();
        Assert.assertTrue(isDirectory, "Destination wasn't created as a directory");

        Long resultSetSize = head.getHttpHeaders().getResultSetSize();
        Assert.assertEquals(resultSetSize.longValue(), 0L,
                "Destination directory is not empty");

        boolean sourceIsDeleted = !mantaClient.existsAndIsAccessible(path);
        Assert.assertTrue(sourceIsDeleted, "Source directory didn't get deleted: "
            + path);
    }


    @Test
    public final void canMoveDirectoryWithContents() throws IOException {
        final String name = "source-" + UUID.randomUUID().toString();
        final String source = testPathPrefix + name + MantaClient.SEPARATOR;
        mantaClient.putDirectory(source);
        final String destination = testPathPrefix + "dest-" + UUID.randomUUID() + "/";

        mantaClient.putDirectory(source + "dir1");
        mantaClient.putDirectory(source + "dir2");
        mantaClient.putDirectory(source + "dir3");

        mantaClient.put(source + "file1.txt", TEST_DATA);
        mantaClient.put(source + "file2.txt", TEST_DATA);
        mantaClient.put(source + "dir1/file3.txt", TEST_DATA);
        mantaClient.put(source + "dir1/file4.txt", TEST_DATA);
        mantaClient.put(source + "dir3/file5.txt", TEST_DATA);

        mantaClient.move(source, destination);
        boolean newLocationExists = mantaClient.existsAndIsAccessible(destination);
        Assert.assertTrue(newLocationExists, "Destination directory doesn't exist: "
                + destination);

        MantaObjectResponse headDestination = mantaClient.head(destination);

        boolean isDirectory = headDestination.isDirectory();
        Assert.assertTrue(isDirectory, "Destination wasn't created as a directory");

        Long resultSetSize = headDestination.getHttpHeaders().getResultSetSize();
        Assert.assertEquals(resultSetSize.longValue(), 5L,
                "Destination directory doesn't have the same number of entries as source");

        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "file1.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "file2.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir2"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir3"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1/file3.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir1/file4.txt"));
        Assert.assertTrue(mantaClient.existsAndIsAccessible(destination + "dir3/file5.txt"));

        boolean sourceIsDeleted = !mantaClient.existsAndIsAccessible(source);
        Assert.assertTrue(sourceIsDeleted, "Source directory didn't get deleted: "
                + source);
    }


    @Test
    public final void testList() throws IOException {
        final String pathPrefix = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(pathPrefix, null);

        mantaClient.put(String.format("%s/%s", pathPrefix, UUID.randomUUID()), "");
        mantaClient.put(String.format("%s/%s", pathPrefix, UUID.randomUUID()), "");
        final String subDir = pathPrefix + "/" + UUID.randomUUID().toString();
        mantaClient.putDirectory(subDir, null);
        mantaClient.put(String.format("%s/%s", subDir, UUID.randomUUID()), "");
        final Stream<MantaObject> objs = mantaClient.listObjects(pathPrefix);

        final AtomicInteger count = new AtomicInteger(0);
        objs.forEach(obj -> {
            count.incrementAndGet();
            Assert.assertTrue(obj.getPath().startsWith(testPathPrefix));
        });

        Assert.assertEquals(3, count.get());
    }


    @Test(expectedExceptions = MantaObjectException.class)
    public final void testListNotADir() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);

        try (Stream<MantaObject> objects = mantaClient.listObjects(path)) {
            objects.count();
        }
    }


    @Test
    public final void testIsDirectoryEmptyWithEmptyDir() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String dir = testPathPrefix + name;
        mantaClient.putDirectory(dir);

        Assert.assertTrue(mantaClient.isDirectoryEmpty(dir),
                "Empty directory is not reported as empty");
    }


    @Test
    public final void testIsDirectoryEmptyWithDirWithFiles() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String dir = testPathPrefix + name;
        mantaClient.putDirectory(dir);

        mantaClient.put(String.format("%s/%s", dir, UUID.randomUUID()), TEST_DATA);

        Assert.assertFalse(mantaClient.isDirectoryEmpty(dir),
                "Empty directory is not reported as empty");
    }


    @Test(expectedExceptions = { MantaClientException.class })
    public final void testIsDirectoryEmptyWithAFileNotDir() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String file = testPathPrefix + name;

        mantaClient.put(file, TEST_DATA);

        mantaClient.isDirectoryEmpty(file);
    }


    @Test
    public final void testRFC3986() throws IOException {
        final String name = "spaces in the name of the file";
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);
        final String actual = mantaClient.getAsString(path);

        Assert.assertEquals(actual, TEST_DATA);
        mantaClient.delete(path);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(testPathPrefix + name));
    }


    @Test
    public final void testFileExists() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        mantaClient.put(path, TEST_DATA);

        final boolean actual = mantaClient.existsAndIsAccessible(path);
        Assert.assertTrue(actual, "File object should exist");
    }


    @Test
    public final void testDirectoryExists() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String dir = testPathPrefix + name;

        mantaClient.putDirectory(dir);

        final boolean actual = mantaClient.existsAndIsAccessible(dir);
        Assert.assertTrue(actual, "File object should exist");
    }


    @Test
    public final void testFileDoesntExist() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;

        final boolean actual = mantaClient.existsAndIsAccessible(path);
        Assert.assertFalse(actual, "File object shouldn't exist");
    }

    @Test(groups = { "mtime" })
    public final void testGetLastModifiedDate() {
        final String mtime = "Wed, 11 Nov 2015 18:20:20 GMT";
        final Date expected = DateUtils.parseDate(mtime);
        final MantaObjectResponse obj = new MantaObjectResponse(testPathPrefix);
        obj.setMtime(mtime);

        Assert.assertEquals(obj.getLastModifiedTime(), expected,
                "Last modified date should equal input to mtime");
    }


    @Test(groups = { "mtime" })
    public final void testGetNullLastModifiedDate() {
        final MantaObjectResponse obj = new MantaObjectResponse(testPathPrefix);
        obj.setMtime(null);

        Assert.assertNull(obj.getLastModifiedTime(),
                "Last modified date should be null when mtime is null");
    }


    @Test(groups = { "mtime" })
    public final void testGetLastModifiedDateWithUnparseableMtime() {
        final String mtime = "Bad unparseable string";
        final MantaObjectResponse obj = new MantaObjectResponse(testPathPrefix);
        obj.setMtime(mtime);

        Assert.assertNull(obj.getLastModifiedTime(),
                "Last modified date should be null when mtime is null");
    }


}
