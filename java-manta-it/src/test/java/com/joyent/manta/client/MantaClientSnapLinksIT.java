/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.*;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.SNAPLINKS_DISABLED_ERROR;
import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Tests the basic functionality of operations related to snaplinks
 * for the {@link MantaClient} class.
 * <p>
 * Since we want to make it possible to run this test without a code change, this test throws a {@link
 * org.testng.SkipException} if snaplinks are disabled for the "manta.user" account, the method
 * {@link MantaClientSnapLinksIT#putSnapLinkAndSkipIfUnsupported(String, String)} will skip this integration-test from running.
 * </p>
 * <p>Remember to also pass system properties for client configuration (manta.user/etc.) or set the values in the
 * environment (MANTA_USER/etc).
 * </p>
 *
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@Test(groups = "snaplinks")
public class MantaClientSnapLinksIT {

    private static final Logger LOG = LoggerFactory.getLogger(MantaClientSnapLinksIT.class);

    private static final String TEST_DATA = "Arise,Awake And Do Not Stop Until Your Goal Is Reached.";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        IntegrationTestConfigContext.cleanupTestDirectory(mantaClient, testPathPrefix);
    }

    @Test(dependsOnMethods = "testPutLink")
    public final void canMoveDirectoryWithContents() throws IOException {
        final String name = "source-" + UUID.randomUUID().toString();
        final String source = testPathPrefix + name + MantaClient.SEPARATOR;
        final String destination = testPathPrefix + "dest-" + UUID.randomUUID() + SEPARATOR;
        moveDirectoryWithContents(source, destination);
    }

    @Test(dependsOnMethods = "testPutLink")
    public final void canMoveFileToDifferentPrecreatedDirectory() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + SEPARATOR;
        mantaClient.putDirectory(newDir);
        final String newPath = newDir + "this-is-a-new-name.txt";

        mantaClient.move(path, newPath);
        final String movedContent = mantaClient.getAsString(newPath);
        Assert.assertEquals(movedContent, TEST_DATA);
    }

    @Test(dependsOnMethods = "testPutLink")
    public final void canMoveFileToDifferentUncreatedDirectoryCreationEnabled() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);
        final String newDir = testPathPrefix + "subdir-" + UUID.randomUUID() + SEPARATOR;

        final String newPath = newDir + "this-is-a-new-name.txt";

        mantaClient.move(path, newPath, true);
        final String movedContent = mantaClient.getAsString(newPath);
        Assert.assertEquals(movedContent, TEST_DATA);
    }

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
        putSnapLinkAndSkipIfUnsupported(testPathPrefix + linkName, testPathPrefix + objectName);
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

    public final void testPutLink() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name;
        mantaClient.put(path, TEST_DATA);

        final String link = UUID.randomUUID().toString();
        putSnapLinkAndSkipIfUnsupported(testPathPrefix + link, testPathPrefix + name);
        final String linkContent = mantaClient.getAsString(testPathPrefix + link);
        Assert.assertEquals(linkContent, TEST_DATA);
    }

    public final void testPutJsonLink() throws IOException {
        final String name = UUID.randomUUID().toString();
        final String path = testPathPrefix + name + ".json";
        final String testData = "{}";

        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType("application/json");
        mantaClient.put(path, testData, headers);

        final String linkPath = testPathPrefix + UUID.randomUUID() + ".json";
        putSnapLinkAndSkipIfUnsupported(linkPath, path);
        final String linkContent = mantaClient.getAsString(linkPath);
        Assert.assertEquals(linkContent, testData);
    }

    // TEST UTILITY METHODS

    private void moveDirectoryWithContents(final String source, final String destination) throws IOException {
        mantaClient.putDirectory(source);

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

    private void putSnapLinkAndSkipIfUnsupported(final String linkPath, final String objectPath) throws IOException {
        try {
            mantaClient.putSnapLink(linkPath, objectPath, null);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(SNAPLINKS_DISABLED_ERROR)) {
                final String message =
                        "This integration-test class can't be run since SnapLinks" +
                                "have been disabled for this account";
                LOG.warn(message);
                throw new SkipException(message, e);
            }

            throw e;
        }
    }
}
