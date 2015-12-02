/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.client.config.TestConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaCryptoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.SkipException;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests the execution of Manta compute jobs.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test(groups = { "job" })
public class MantaClientJobIT {
    private static final Logger LOG = LoggerFactory.getLogger(MantaClientJobIT.class);

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
    public void canAddAndGetInputsFromIterator() throws IOException {
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
            List<String> inputsResponse = mantaClient.getJobInputs(jobId)
                    .collect(Collectors.toList());
            Assert.assertEquals(inputsResponse, inputs);
        } finally {
            mantaClient.cancelJob(jobId);
        }
    }

    @Test(dependsOnMethods = { "createJob" })
    public void canAddAndGetInputsFromStream() throws IOException {
        MantaJob job = buildJob();
        UUID jobId = mantaClient.createJob(job);

        List<String> inputs = new ArrayList<>();
        String objPath = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.put(objPath, TEST_DATA);
        inputs.add(objPath);

        try {
            mantaClient.addJobInputs(jobId, inputs.stream());
            boolean ended = mantaClient.endJobInput(jobId);
            Assert.assertTrue(ended, "Ending input wasn't accepted");
            List<String> inputsResponse = mantaClient.getJobInputs(jobId)
                    .collect(Collectors.toList());
            Assert.assertEquals(inputsResponse, inputs);
        } finally {
            mantaClient.cancelJob(jobId);
        }
    }

    @Test(dependsOnMethods = { "createJob", "getJob" })
    public void canListAllJobIDs() throws IOException, InterruptedException {
        final MantaJob job1 = buildJob();
        final UUID job1id = mantaClient.createJob(job1);
        final MantaJob job2 = buildJob();
        final UUID job2id = mantaClient.createJob(job2);

        mantaClient.endJobInput(job1id);
        mantaClient.endJobInput(job2id);

        while (!mantaClient.getJob(job1id).getState().equals("done")) {
            Thread.sleep(1000);
        }

        while (!mantaClient.getJob(job2id).getState().equals("done")) {
            Thread.sleep(1000);
        }

        try (Stream<UUID> jobs = mantaClient.getAllJobIds()) {
            List<UUID> found = jobs.filter(id -> id.equals(job1id) || id.equals(job2id))
                                   .collect(Collectors.toList());

            Assert.assertEquals(found.size(), 2, "We should have found both jobs");
        }  catch (AssertionError e) {
            String msg = "Couldn't find job in job list, retry test a few times to verify";
            LOG.error(msg, e);
            throw new SkipException(msg, e);
        }
    }

    @Test(dependsOnMethods = { "createJob", "getJob" })
    public void canListAllRunningJobIDs() throws IOException, InterruptedException {
        final MantaJob job1 = buildJob();
        final UUID job1id = mantaClient.createJob(job1);
        final MantaJob job2 = buildJob();
        final UUID job2id = mantaClient.createJob(job2);
        final MantaJob job3 = buildJob();
        final UUID job3id = mantaClient.createJob(job3);

        mantaClient.endJobInput(job3id);

        while (!mantaClient.getJob(job3id).getState().equals("done")) {
            Thread.sleep(1000);
        }

        List<UUID> searchIds = new ArrayList<>();
        searchIds.add(job1id);
        searchIds.add(job2id);
        searchIds.add(job3id);

        try (Stream<UUID> jobs = mantaClient.getJobIdsByState("running")) {
            List<UUID> found = jobs.filter(searchIds::contains)
                    .collect(Collectors.toList());

            Assert.assertEquals(found.size(), 2, "We should have found both jobs");
        } catch (AssertionError e) {
            String msg = "Couldn't find job in job list, retry test a few times to verify";
            LOG.error(msg, e);
            throw new SkipException(msg, e);
        } finally {
            mantaClient.cancelJob(job1id);
            mantaClient.cancelJob(job2id);
        }
    }

    @Test(dependsOnMethods = { "createJob", "getJob" })
    public void canListAllRunningJobs() throws IOException, InterruptedException {
        final MantaJob job1 = buildJob();
        final UUID job1id = mantaClient.createJob(job1);
        final MantaJob job2 = buildJob();
        final UUID job2id = mantaClient.createJob(job2);
        final MantaJob job3 = buildJob();
        final UUID job3id = mantaClient.createJob(job3);

        mantaClient.endJobInput(job3id);

        while (!mantaClient.getJob(job3id).getState().equals("done")) {
            Thread.sleep(1000);
        }

        List<UUID> searchIds = new ArrayList<>();
        searchIds.add(job1id);
        searchIds.add(job2id);
        searchIds.add(job3id);

        try (Stream<MantaJob> jobs = mantaClient.getJobsByState("running")) {
            List<MantaJob> found = jobs.filter(j -> searchIds.contains(j.getId()))
                    .collect(Collectors.toList());

            Assert.assertEquals(found.size(), 2, "We should have found both jobs");
        } catch (AssertionError e) {
            String msg = "Couldn't find job in job list, retry test a few times to verify";
            LOG.error(msg, e);
            throw new SkipException(msg, e);
        } finally {
            mantaClient.cancelJob(job1id);
            mantaClient.cancelJob(job2id);
        }
    }

    @Test(dependsOnMethods = { "createJob", "getJob" })
    public void canListJobsByName() throws IOException {
        final String name = String.format("by_name_%s", UUID.randomUUID());
        final MantaJob job1 = buildJob(name);
        final UUID job1id = mantaClient.createJob(job1);
        MantaJob job2 = buildJob(name);
        final UUID job2id = mantaClient.createJob(job2);

        try (Stream<UUID> jobs = mantaClient.getJobIdsByName(name)) {
            List<UUID> found = jobs.collect(Collectors.toList());

            Assert.assertEquals(found.size(), 2, "We should have found both jobs");
            Assert.assertTrue(found.contains(job1id));
            Assert.assertTrue(found.contains(job2id));
        } catch (AssertionError e) {
            String msg = "Couldn't find job in job list, retry test a few times to verify";
            LOG.error(msg, e);
            throw new SkipException(msg, e);
        } finally {
            mantaClient.cancelJob(job1id);
            mantaClient.cancelJob(job2id);
        }
    }

    @Test
    public void canListOutputsForJobWithNoInputs() throws IOException, InterruptedException {
        final MantaJob job = buildJob();
        final UUID jobId = mantaClient.createJob(job);
        mantaClient.endJobInput(jobId);

        while (!mantaClient.getJob(jobId).getState().equals("done")) {
            Thread.sleep(1000);
        }

        List<String> outputs = mantaClient.getJobOutputs(jobId)
                .collect(Collectors.toList());

        Assert.assertEquals(outputs.size(), 0);
    }

    @Test
    public void canListOutputsForJobWithOneInput() throws IOException, InterruptedException {
        String path = String.format("%s/%s", testPathPrefix, UUID.randomUUID());
        mantaClient.put(path, TEST_DATA);

        final MantaJob job = buildJob();
        final UUID jobId = mantaClient.createJob(job);

        List<String> inputs = new ArrayList<>();
        inputs.add(path);

        mantaClient.addJobInputs(jobId, inputs.iterator());
        mantaClient.endJobInput(jobId);

        while (!mantaClient.getJob(jobId).getState().equals("done")) {
            Thread.sleep(1000);
        }

        List<String> outputs = mantaClient.getJobOutputs(jobId)
                .collect(Collectors.toList());

        Assert.assertEquals(outputs.size(), 1);

        String actual = mantaClient.getAsString(outputs.get(0));
        Assert.assertEquals(actual, TEST_DATA,
                "Output wasn't the same as input");
    }

    private MantaJob buildJob() {
        String name = String.format("integration_test_%d", count.incrementAndGet());
        return buildJob(name);
    }

    private MantaJob buildJob(final String name) {
        List<MantaJobPhase> phases = new ArrayList<>();
        MantaJobPhase map = new MantaJobPhase()
            .setType("map")
            .setExec("cat");
        phases.add(map);

        return new MantaJob(name, phases);
    }
}
