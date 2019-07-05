/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Tests the execution of Manta compute jobs using the builder fluent interface.
 */
@Test
public class MantaJobBuilderIT {
    private static final String TEST_DATA =
              "line 01 aa\n"
            + "line 02 bb\n"
            + "line 03 aa\n"
            + "line 04 bb\n"
            + "line 05 aa\n"
            + "line 06 bb\n"
            + "line 07 aa\n"
            + "line 08 bb\n"
            + "line 09 aa\n"
            + "line 10 bb";

    private String testPathPrefix;

    private MantaClient mantaClient;

    @BeforeClass
    public void beforeClass() throws IOException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new IntegrationTestConfigContext();

        mantaClient = new MantaClient(config);

        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());
        mantaClient.putDirectory(testPathPrefix, true);
    }

    @AfterClass
    public void cleanup() throws IOException {
        IntegrationTestConfigContext.cleanupTestDirectory(mantaClient, testPathPrefix);
    }

    @Test
    public void canBuildTypicalJob() throws IOException {
        final MantaJobBuilder builder = mantaClient.jobBuilder();

        String jobName = String.format("job_%s", UUID.randomUUID());
       
        String path1 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s%s", testPathPrefix, UUID.randomUUID());

        mantaClient.put(path1, TEST_DATA);
        mantaClient.put(path2, TEST_DATA);
        mantaClient.put(path3, TEST_DATA);

        MantaJobBuilder.Done finishedJob = builder.newJob(jobName)
               .addPhase(new MantaJobPhase()
                            .setExec("grep bb")
                            .setType("map"))
                .addPhase(new MantaJobPhase()
                        .setExec("sort | uniq")
                        .setType("reduce"))
               .addInputs(path1, path2, path3)
               .validateInputs()
               .run()
               .waitUntilDone();

        List<String> outputs = finishedJob
                .validateJobsSucceeded()
                .outputs()
                .collect(Collectors.toList());

        Assert.assertEquals(outputs.size(), 1, "The job should have reduced to 1 output");

        try (Reader reader = new StringReader(outputs.get(0));
             BufferedReader br = new BufferedReader(reader)) {
            List<String> lines = br.lines().collect(Collectors.toList());

            Assert.assertEquals(lines.size(), 5, "The output should have 5 lines");
            Assert.assertTrue(lines.contains("line 02 bb"));
            Assert.assertTrue(lines.contains("line 04 bb"));
            Assert.assertTrue(lines.contains("line 06 bb"));
            Assert.assertTrue(lines.contains("line 08 bb"));
            Assert.assertTrue(lines.contains("line 10 bb"));
        }
    }

    @Test
    public void cantRerunOldJob() throws IOException {
        final MantaJobBuilder builder = mantaClient.jobBuilder();

        String jobName = String.format("job_%s", UUID.randomUUID());

        String path1 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s%s", testPathPrefix, UUID.randomUUID());

        mantaClient.put(path1, TEST_DATA);
        mantaClient.put(path2, TEST_DATA);
        mantaClient.put(path3, TEST_DATA);

        MantaJobBuilder.Done finishedJob = builder.newJob(jobName)
                .addPhase(new MantaJobPhase()
                        .setExec("grep bb")
                        .setType("map"))
                .addPhase(new MantaJobPhase()
                        .setExec("sort | uniq")
                        .setType("reduce"))
                .addInputs(path1, path2, path3)
                .validateInputs()
                .run()
                .waitUntilDone();

        finishedJob.validateJobsSucceeded();

        UUID jobId = finishedJob.getJob().getId();

        boolean failed = false;

        try {
            mantaClient.endJobInput(jobId);
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(MantaErrorCode.INVALID_JOB_STATE_ERROR)) {
                failed = true;
            }
        }

        Assert.assertTrue(failed, "Rerunning completed job worked for some reason");
    }

    @Test
    public void canCloneJob() throws IOException {
        final MantaJobBuilder builder = mantaClient.jobBuilder();

        String jobName = String.format("job_%s", UUID.randomUUID());

        String path1 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s%s", testPathPrefix, UUID.randomUUID());

        mantaClient.put(path1, TEST_DATA);
        mantaClient.put(path2, TEST_DATA);
        mantaClient.put(path3, TEST_DATA);

        MantaJobBuilder.Done finishedJob = builder.newJob(jobName)
                .addPhase(new MantaJobPhase()
                        .setExec("grep bb")
                        .setType("map"))
                .addPhase(new MantaJobPhase()
                        .setExec("sort | uniq")
                        .setType("reduce"))
                .addInputs(path1, path2, path3)
                .validateInputs()
                .run()
                .waitUntilDone();

        List<String> outputs = finishedJob
                .validateJobsSucceeded()
                .outputs()
                .collect(Collectors.toList());

        MantaJob oldJob = finishedJob.getJob();

        MantaJobBuilder.Done clonedJob = builder.cloneJob(oldJob)
                .validateInputs()
                .run()
                .waitUntilDone();

        List<String> clonedOutputs = clonedJob
                .validateJobsSucceeded()
                .outputs()
                .collect(Collectors.toList());

        Assert.assertEquals(clonedOutputs, outputs,
                "Expected the same output as the original job");
    }
}
