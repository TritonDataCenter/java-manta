/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.IntegrationTestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaErrorCode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tests the execution of Manta compute jobs using the builder fluent interface.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
//@Test(dependsOnGroups = "job")
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

    private AtomicInteger count = new AtomicInteger(0);

    private MantaClient mantaClient;

    @BeforeClass
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

        testPathPrefix = String.format("%s/stor/%s",
                config.getMantaHomeDirectory(), UUID.randomUUID());
        mantaClient.putDirectory(testPathPrefix, null);
    }

    @AfterClass
    public void cleanup() throws IOException {
        if (mantaClient != null) {
            mantaClient.deleteRecursive(testPathPrefix);
            mantaClient.closeQuietly();
        }
    }

    @Test
    public void canBuildTypicalJob() throws IOException {
        final MantaJobBuilder builder = mantaClient.jobBuilder();

        String jobName = String.format("job_%s", UUID.randomUUID());

        String path1 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

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

        String path1 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

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

        String path1 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path2 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        String path3 = String.format("%s/%s", testPathPrefix, UUID.randomUUID());

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
