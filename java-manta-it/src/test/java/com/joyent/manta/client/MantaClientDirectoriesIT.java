/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;
import static com.joyent.manta.util.MantaUtils.writeablePrefixPaths;

/**
 * Tests for verifying the correct functioning of making remote requests
 * against Manta directories.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "directory" })
public class MantaClientDirectoriesIT {

    private static final Logger LOG = LoggerFactory.getLogger(MantaClientDirectoriesIT.class);

    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    private IntegrationTestConfigContext config;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {

        config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        String testPathBase = String.format("%s/stor/java-manta-integration-tests",
                config.getMantaHomeDirectory());
        testPathPrefix = String.format("%s/%s",
                testPathBase, UUID.randomUUID());
        mantaClient.putDirectory(testPathBase, true);
    }

    @AfterClass
    public void afterClass() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeWithWarning();
        }
    }

    @Test
    public void canCreateDirectory() throws IOException {
        mantaClient.putDirectory(testPathPrefix);

        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        boolean created = mantaClient.putDirectory(dir);

        Assert.assertTrue(created, "Directory was marked as created");

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());
    }

    @Test
    public void willReturnFalseWhenWeOverwriteDirectory() throws IOException {
        mantaClient.putDirectory(testPathPrefix);

        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        Assert.assertTrue(mantaClient.putDirectory(dir),
                "We were unable to create the initial directory");

        boolean result = mantaClient.putDirectory(dir);

        Assert.assertFalse(result, "Expected a false value because we "
                + "didn't create a new directory");
    }

    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void canDeleteDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());

        mantaClient.delete(dir);

        MantaAssert.assertResponseFailureStatusCode(404, RESOURCE_NOT_FOUND_ERROR,
                (MantaFunction<Object>) () -> mantaClient.get(dir));
    }

    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void wontErrorWhenWeCreateOverAnExistingDirectory() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);
        mantaClient.putDirectory(dir);
        mantaClient.putDirectory(dir);

        MantaObject response = mantaClient.head(dir);
        Assert.assertEquals(dir, response.getPath());
    }

    /**
     * This is somewhat surprising behavior, but this test documents Manta's
     * behavior. As a user of Manta, you will need to check to see if a
     * file exists before attempting to write a directory over the top of it.
     */
    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void noErrorWhenWeOverwriteAnExistingFile() throws IOException {
        String dir = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.putDirectory(dir);

        String file = String.format("%s/%s", dir, UUID.randomUUID());
        mantaClient.put(file, TEST_DATA);
        mantaClient.putDirectory(file);
    }

    @Test(dependsOnMethods = { "canCreateDirectory" })
    public void directoryIsMarkedAsSuch() throws IOException {
        MantaObject dir = mantaClient.head(testPathPrefix);
        Assert.assertTrue(dir.isDirectory(),
                String.format("Directory should be marked as such [%s]. "
                        + "\nResponse: %s", testPathPrefix, dir));
    }

    @Test(dependsOnMethods = { "wontErrorWhenWeCreateOverAnExistingDirectory" })
    public void canRecursivelyCreateDirectory() throws IOException {
        String dir = String.format("%s/%s/%s/%s/%s/%s", testPathPrefix,
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID());

        mantaClient.putDirectory(dir, true);

        MantaObject response = mantaClient.head(dir);

        Assert.assertTrue(response.isDirectory(),
                String.format("Directory should be marked as such [%s]", testPathPrefix));
        Assert.assertEquals(dir, response.getPath());
    }

    @Test
    public void canCreateDirectoriesNormallyWhenNewDirectoryDepthLessThanSkipDepth() throws IOException {
        if (!mantaClient.existsAndIsAccessible(testPathPrefix)) {
            Assert.fail("Base directory is missing");
        }

        final String childDirectory = testPathPrefix + SEPARATOR + UUID.randomUUID();
        final int childDirectoryDepth = writeablePrefixPaths(childDirectory).length;
        // set the skip depth to be greater than the new directory's depth
        config.setSkipDirectoryDepth(childDirectoryDepth + 1);

        final long operations =
                RecursiveDirectoryCreationStrategy.createWithSkipDepth(mantaClient, childDirectory, null, config.getSkipDirectoryDepth());

        // check that number of operations is exactly the same as the writeable directories
        // (i.e. no extra operations and no fewer operations)
        Assert.assertEquals(operations, childDirectoryDepth);
    }

    @Test
    public void canSkipAlreadyCreatedDirectoriesWhenDepthSetCorrectly() throws IOException {
        directorySkipping(true);
    }

    @Test
    public void canAddAtMostOneFailedOperationWhenDepthSetIncorrectly() throws IOException {
        directorySkipping(false);
    }

    private void directorySkipping(final boolean settingCorrectDepth) throws IOException {
        final int parentAddedDepth = RandomUtils.nextInt(1, 5);
        final int childAddedDepth = RandomUtils.nextInt(1, 5);

        final StringBuilder parentDirBuilder = new StringBuilder(testPathPrefix);
        for (int i = 0; i < parentAddedDepth; i++) {
            parentDirBuilder
                    .append(SEPARATOR)
                    .append(RandomStringUtils.random(3, true, false));
        }
        final String parentDir = parentDirBuilder.toString();
        final int parentDirDepth = writeablePrefixPaths(parentDir).length;
        config.setSkipDirectoryDepth(parentDirDepth);

        if (!settingCorrectDepth) {
            // assume the directory _directly_ under the parent exists so we cause a failure
            config.setSkipDirectoryDepth(config.getSkipDirectoryDepth() + 1);
        }

        final StringBuilder childDirBuilder = new StringBuilder(parentDir);
        for (int i = 0; i < childAddedDepth; i++) {
            childDirBuilder
                    .append(SEPARATOR)
                    .append(RandomStringUtils.random(3, true, false));
        }
        final String childDir = childDirBuilder.toString();

        mantaClient.putDirectory(parentDir, true);
        final MantaObject response = mantaClient.head(parentDir);
        Assert.assertTrue(response.isDirectory(),
                String.format("Directory should be marked as such [%s]", testPathPrefix));
        Assert.assertEquals(parentDir, response.getPath());

        // create the strategy ourselves so we can check the number of operations
        final long operations =
                RecursiveDirectoryCreationStrategy.createWithSkipDepth(mantaClient, childDir, null, config.getSkipDirectoryDepth());

        // ensure that the child was created
        final MantaObject nestedResponse = mantaClient.head(childDir);
        Assert.assertTrue(nestedResponse.isDirectory(),
                String.format("Nested directory should be marked as such [%s]", testPathPrefix));
        Assert.assertEquals(childDir, nestedResponse.getPath());

        // verify that created the nested directory took less calls than its parent
        LOG.info("PARENT " + parentDir);
        LOG.info("CHILD  " + childDir);
        LOG.info("calls (child): " + operations);

        if (settingCorrectDepth) {
            // best case we only create directories belonging to the child
            Assert.assertEquals(operations, childAddedDepth);
        } else {
            // when we fail we'll do a single (unsuccessful) probe directly under the assumed parent depth plus one PUT
            // for each directory the in child path
            Assert.assertEquals(operations, parentDirDepth + childAddedDepth + 1);
        }
    }

}
