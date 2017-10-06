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
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

@Test
public class MantaClientFindIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    @Parameters({"lazy"})
    public void beforeClass(@Optional Boolean lazy) throws IOException {
        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();

        mantaClient = MantaClientFactory.build(config, lazy);
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

    /**
     * This test determines if the find() method can find a trivial amount of
     * files and directories.
     */
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

        List<String> allObjects = new LinkedList<>();
        allObjects.addAll(level1Dirs);
        allObjects.addAll(level1Files);
        allObjects.addAll(level2Files);

        final List<MantaObject> results;

        try (Stream<MantaObject> stream = mantaClient.find(testPathPrefix)) {
            results = stream.collect(Collectors.toList());
        }

        Assert.assertFalse(results.isEmpty(),
                "We should have many objects returned");
        Assert.assertEquals(results.size(), allObjects.size(),
                "We should return exactly as many objects as added");

        for (String expectedObject : allObjects) {
            try (Stream<MantaObject> resultsStream = results.stream()) {
                long count = resultsStream
                        .filter(obj -> obj.getPath().equals(expectedObject)).count();
                Assert.assertEquals(count, 1L);
            }
        }
    }

    /**
     * This test determines that we are filtering results as per our expection
     * when using a filter predicate with find().
     */
    public void canFindRecursivelyWithFilter() throws IOException {
        List<String> level1Dirs = Arrays.asList(
                testPathPrefix + "aaa_bbb_ccc", testPathPrefix + "aaa_111_ccc",
                testPathPrefix + UUID.randomUUID()
        );

        List<String> level1Files = Arrays.asList(
                testPathPrefix + UUID.randomUUID(), testPathPrefix + "aaa_222_ccc"
        );

        for (String dir : level1Dirs) {
            mantaClient.putDirectory(dir);
        }

        for (String file : level1Files) {
            mantaClient.put(file, TEST_DATA, StandardCharsets.UTF_8);
        }

        List<String> level2Files = level1Dirs
                .stream()
                .flatMap(dir -> Stream.of(
                        dir + SEPARATOR + "aaa_333_ccc",
                        dir + SEPARATOR + "aaa_444_ccc",
                        dir + SEPARATOR + UUID.randomUUID()))
                .collect(Collectors.toList());

        for (String file : level2Files) {
            mantaClient.put(file, TEST_DATA, StandardCharsets.UTF_8);
        }

        final String[] results;

        Predicate<? super MantaObject> filter = (Predicate<MantaObject>) obj ->
                FilenameUtils.getName(obj.getPath()).startsWith("aaa_");

        try (Stream<MantaObject> stream = mantaClient.find(testPathPrefix, filter)) {
            Stream<String> paths = stream.map(MantaObject::getPath);
            Stream<String> sorted = paths.sorted();
            results = sorted.toArray(String[]::new);
        }

        String[] expected = new String[] {
                testPathPrefix + "aaa_111_ccc",
                testPathPrefix + "aaa_111_ccc" + SEPARATOR + "aaa_333_ccc",
                testPathPrefix + "aaa_111_ccc" + SEPARATOR + "aaa_444_ccc",
                testPathPrefix + "aaa_222_ccc",
                testPathPrefix + "aaa_bbb_ccc",
                testPathPrefix + "aaa_bbb_ccc" + SEPARATOR + "aaa_333_ccc",
                testPathPrefix + "aaa_bbb_ccc" + SEPARATOR + "aaa_444_ccc",
        };

        try {
            Assert.assertEqualsNoOrder(results, expected);
        } catch (AssertionError e) {
            System.err.println("ACTUAL:   " + StringUtils.join(results, ", "));
            System.err.println("EXPECTED: " + StringUtils.join(expected, ", "));
            throw e;
        }
    }

    /**
     * This test determines that the find() method can recurse to a reasonable
     * number of subdirectories without throwing a {@link StackOverflowError}.
     *
     * As you go over 70 subdirectories, Manta will throw a 500 error. We should
     * be able to recurse to a stack depth far deeper than Manta can support
     * for subdirectories.
     */
    public void findCanDeeplyRecurse() throws IOException {
        final int depth = 70;
        final StringBuilder path = new StringBuilder(testPathPrefix);
        mantaClient.putDirectory(path.toString());

        final int currentDepth = StringUtils.countMatches(testPathPrefix, SEPARATOR);

        // We start with i=currentDepth because we are already currentDepth levels deep
        for (int i = currentDepth; i < depth; i++) {
            path.append(UUID.randomUUID() + SEPARATOR);
            mantaClient.putDirectory(path.toString());
            String file = path + String.format("subdirectory-file-%d.txt", i);
            mantaClient.put(file, TEST_DATA, StandardCharsets.UTF_8);
        }

        mantaClient.find(path.toString());
    }

    /**
     * This test sees if find() can find a large number of files spread across
     * many directories. It validates the results against the node.js Manta CLI
     * tool mls.
     *
     * This test is disabled by default because it is difficult to know if the
     * running system has the node.js CLI tools properly installed. Please run
     * this test manually on an as-needed basis.
     */
    @Test(enabled = false)
    public void findMatchesMfind() throws SecurityException, IOException, InterruptedException {
        ConfigContext context = mantaClient.getContext();
        String reports = context.getMantaHomeDirectory()
                + SEPARATOR + "reports" + SEPARATOR + "usage" + SEPARATOR + "summary";
        String[] cmd = new String[] { "mfind", reports };

        ProcessBuilder processBuilder = new ProcessBuilder(cmd);

        Map<String, String> env = processBuilder.environment();
        env.put("MANTA_URL", context.getMantaURL());
        env.put("MANTA_USER", context.getMantaUser());
        env.put("MANTA_KEY_ID", context.getMantaKeyId());

        long mFindStart = System.nanoTime();
        Process process = processBuilder.start();

        String charsetName = StandardCharsets.UTF_8.name();

        List<String> objects = new LinkedList<>();

        try (Scanner scanner = new Scanner(process.getInputStream(), charsetName)) {
            while (scanner.hasNextLine()) {
                objects.add(scanner.nextLine());
            }
        }

        System.err.println("Waiting for mfind to complete");
        Assert.assertEquals(process.waitFor(), 0,
                "mfind exited with an error");
        long mFindEnd = System.nanoTime();
        System.err.printf("mfind process completed in %d ms\n",
                Duration.ofNanos(mFindEnd - mFindStart).toMillis());

        long findStart = System.nanoTime();

        List<String> foundObjects;
        try (Stream<MantaObject> findStream = mantaClient.find(reports)) {
            foundObjects= findStream.map(MantaObject::getPath).collect(Collectors.toList());
        }

        long findEnd = System.nanoTime();
        System.err.printf("find() completed in %d ms\n",
                Duration.ofNanos(findEnd - findStart).toMillis());

        Assert.assertEqualsNoOrder(objects.toArray(), foundObjects.toArray());
    }
}
