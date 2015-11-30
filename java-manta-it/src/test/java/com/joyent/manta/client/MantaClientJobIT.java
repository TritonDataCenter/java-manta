/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests the execution of Manta compute jobs.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "job" })
public class MantaClientJobIT {
    private static final String TEST_DATA = "EPISODEII_IS_BEST_EPISODE";

    private String testPathPrefix;

    private AtomicInteger count = new AtomicInteger(0);

    private MantaClient mantaClient;


    @BeforeClass
    @Parameters({"manta.url", "manta.user", "manta.key_path", "manta.key_id", "manta.timeout"})
    public void beforeClass(@Optional String mantaUrl,
                            @Optional String mantaUser,
                            @Optional String mantaKeyPath,
                            @Optional String mantaKeyId,
                            @Optional Integer mantaTimeout)
            throws IOException, MantaCryptoException {

        // Let TestNG configuration take precedence over environment variables
        ConfigContext config = new TestConfigContext(
                mantaUrl, mantaUser, mantaKeyPath, mantaKeyId, mantaTimeout);

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
    public void createJob() throws IOException {
        MantaJob job = buildJob();
        UUID jobId = mantaClient.createJob(job);
        Assert.assertNotNull(jobId);

        boolean accepted = mantaClient.cancelJob(jobId);
        Assert.assertTrue(accepted, "Cancel request was not accepted");
    }

    @Test(dependsOnMethods = { "createJob" })
    public void getJob() throws IOException {
        MantaJob job = buildJob();
        UUID jobId = mantaClient.createJob(job);
        MantaJob jobResponse = mantaClient.getJob(jobId);

        Assert.assertEquals(jobResponse.getName(),
                job.getName());
        MantaJobPhase mapPhaseRequest = job.getMapPhases().get(0);
        MantaJobPhase mapPhaseResponse = jobResponse.getMapPhases().get(0);

        Assert.assertEquals(mapPhaseResponse.getType(),
                mapPhaseRequest.getType());
        Assert.assertEquals(mapPhaseResponse.getExec(),
                mapPhaseRequest.getExec());

        Assert.assertFalse(jobResponse.getCancelled());
        Assert.assertFalse(jobResponse.getInputDone());
        Assert.assertNotNull(jobResponse.getTimeCreated());
        Assert.assertNull(jobResponse.getTimeDone());

        mantaClient.cancelJob(jobId);
    }

    @Test(dependsOnMethods = { "createJob", "getJob" })
    public void cancelJob() throws IOException {
        MantaJob job = buildJob();
        UUID jobId = mantaClient.createJob(job);
        mantaClient.cancelJob(jobId);

        MantaJob jobResponse = mantaClient.getJob(jobId);

        Assert.assertEquals(jobResponse.getName(),
                job.getName());
        MantaJobPhase mapPhaseRequest = job.getMapPhases().get(0);
        MantaJobPhase mapPhaseResponse = jobResponse.getMapPhases().get(0);

        Assert.assertEquals(mapPhaseResponse.getType(),
                mapPhaseRequest.getType());
        Assert.assertEquals(mapPhaseResponse.getExec(),
                mapPhaseRequest.getExec());

        Assert.assertTrue(jobResponse.getCancelled());
        Assert.assertTrue(jobResponse.getInputDone());
        Assert.assertNotNull(jobResponse.getTimeCreated());
    }


    @Test(dependsOnMethods = { "createJob" })
    public void canAddAndGetInputs() throws IOException {
        MantaJob job = buildJob();
        UUID jobId = mantaClient.createJob(job);

        List<String> inputs = new ArrayList<>();
        String objPath = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.put(objPath, TEST_DATA);
        inputs.add(objPath);

        try {
            mantaClient.addJobInputs(jobId, inputs.iterator());
            boolean ended = mantaClient.endJobInput(jobId);
            Assert.assertTrue(ended, "Ending input wasn't accepted");
            List<String> inputsResponse = mantaClient.getJobInputs(jobId);
            Assert.assertEquals(inputsResponse, inputs);
        } finally {
            mantaClient.cancelJob(jobId);
        }
    }

    private MantaJob buildJob() {
        List<MantaJobPhase> phases = new ArrayList<>();
        MantaJobPhase map = new MantaJobPhase();
        map.setType("map");
        map.setExec("echo 'Hello World'");
        phases.add(map);

        String name = String.format("integration_test_%d", count.incrementAndGet());
        return new MantaJob(name, phases);
    }
}
