/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
@Test
public class MantaClientFindIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

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

    @AfterMethod
    public void cleanUp() throws IOException {
        mantaClient.deleteRecursive(testPathPrefix);
        mantaClient.putDirectory(testPathPrefix, true);
    }

    public void canFindASingleFile() throws IOException {
        String filePath = testPathPrefix + UUID.randomUUID();
        mantaClient.put(filePath, TEST_DATA, StandardCharsets.UTF_8);

        try (Stream<MantaObject> stream = mantaClient.find(testPathPrefix)) {
            List<MantaObject> results = stream.collect(Collectors.toList());

            Assert.assertFalse(results.isEmpty(), "We should have at least one file returned");
            Assert.assertEquals(results.get(0).getPath(), filePath);
            Assert.assertFalse(results.get(0).isDirectory());
        }
    }

    public void canFindASingleDirectory() throws IOException {
        String dirPath = testPathPrefix + UUID.randomUUID();
        mantaClient.putDirectory(dirPath);

        try (Stream<MantaObject> stream = mantaClient.find(testPathPrefix)) {
            List<MantaObject> results = stream.collect(Collectors.toList());

            Assert.assertFalse(results.isEmpty(), "We should have at least one directory returned");
            Assert.assertEquals(results.get(0).getPath(), dirPath);
            Assert.assertTrue(results.get(0).isDirectory());
        }
    }

    public void canFindRecursiveDirectoriesAndFiles() throws IOException {
        List<String> level1Dirs = Arrays.asList(
                testPathPrefix + UUID.randomUUID(), testPathPrefix + UUID.randomUUID(),
                testPathPrefix + UUID.randomUUID()
        );

        List<String> level1Files = Arrays.asList(
                testPathPrefix + UUID.randomUUID(), testPathPrefix + UUID.randomUUID()
        );

        for (String dir : level1Dirs) {
            mantaClient.putDirectory(dir);
        }

        for (String file : level1Files) {
            mantaClient.put(file, TEST_DATA, StandardCharsets.UTF_8);
        }

        List<String> level2Files = level1Dirs
                .stream()
                .flatMap(dir -> Stream.of(dir + SEPARATOR + UUID.randomUUID(),
                                          dir + SEPARATOR + UUID.randomUUID()))
                .collect(Collectors.toList());

        for (String file : level2Files) {
            mantaClient.put(file, TEST_DATA, StandardCharsets.UTF_8);
        }

        int totalObjects = level1Dirs.size() + level1Files.size() + level2Files.size();

        try (Stream<MantaObject> stream = mantaClient.find(testPathPrefix)) {
            List<MantaObject> results = stream.collect(Collectors.toList());

            Assert.assertFalse(results.isEmpty(),
                    "We should have many objects returned");
            Assert.assertEquals(results.size(), totalObjects,
                    "We should return exactly as many objects as added");

            Stream<MantaObject> resultsStream = results.stream();

            List<String> allObjects = new LinkedList<>();
            allObjects.addAll(level1Dirs);
            allObjects.addAll(level1Files);
            allObjects.addAll(level2Files);

            for (String dir : level1Dirs) {
                Assert.assertEquals(
                        resultsStream.filter(obj -> obj.getPath().equals(dir)).count(),
                        1);
            }
        }
    }
}
