/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.test.util.MantaAssert;
import com.joyent.test.util.MantaFunction;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.UUID;

import static com.joyent.manta.exception.MantaErrorCode.RESOURCE_NOT_FOUND_ERROR;

/**
 * Tests for verifying the correct functioning of making remote requests
 * against Manta directories.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "directory" })
public class MantaClientDirectoriesIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"usingEncryption"})
    public void beforeClass(@Optional Boolean usingEncryption) throws IOException {
        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext(usingEncryption);

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config);
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
        mantaClient.putDirectory(testPathPrefix, true);

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
}
