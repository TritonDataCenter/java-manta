/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;
import com.joyent.manta.exception.MantaJobException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class that provides a fluent interface for building jobs. This class
 * doesn't provide any new functionality from what is available in
 * {@link MantaClient}. However, it does provide a very easy way of
 * programmatically creating new jobs. The purpose of this class is not
 * to replicate all of the job functionality on {@link MantaClient}, but
 * rather to provide a subset of it in an easy to use API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("checkstyle:finalclass")
public class MantaJobBuilder {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaJobBuilder.class);

    /**
     * Reference to the {@link MantaClient} used to execute jobs.
     */
    private final MantaClient client;

    /**
     * <p><em>WARNING: Avoid creating an instance using this constructor. If you do,
     * you will need to close this job builder instance independently of
     * any {@link MantaClient} instances.</em></p>
     *
     * <p>Thus, to keep it simple we provide a method on
     * {@link MantaClient#jobBuilder()} that allows
     * for creating a {@link MantaJobBuilder}. Use that method instead of this
     * constructor when building job builder instances.</p>
     *
     * @param client Reference to the {@link MantaClient} used to execute jobs
     */
    public MantaJobBuilder(final MantaClient client) {
        this.client = client;
    }

    /**
     * Create a new job with the specified name.
     * @param name name of job
     * @return a fluent interface providing job creation options
     */
    public Create newJob(final String name) {
        return new Create(this, name);
    }

    /**
     * Returns a reference to a running/completed job as a builder object.
     *
     * @param job job object of completed job
     * @return a fluent interface providing job management options
     */
    public Run lookupJob(final MantaJob job) {
        Validate.notNull(job, "Job must not be null");

        return lookupJob(job.getId());
    }

    /**
     * Returns a reference to a running/completed job as a builder object.
     *
     * @param jobId job id of completed job
     * @return a fluent interface providing job management options
     */
    public Run lookupJob(final UUID jobId) {
        Validate.notNull("Job id must not be null");

        return new MantaJobBuilder.Run(this, jobId);
    }

    /**
     * Clones a completed job as a new job by referencing its id.
     *
     * @param jobId job id of completed job
     * @return a fluent interface providing job creation options
     * @throws IOException thrown when we can't find the job referenced
     */
    public Create cloneJob(final UUID jobId) throws IOException {
        final MantaJob job = client.getJob(jobId);
        return cloneJob(job);
    }

    /**
     * Clones a completed job as a new job.
     *
     * @param job job object of completed job
     * @return a fluent interface providing job creation options
     * @throws IOException thrown when we can't find the job referenced
     */
    public Create cloneJob(final MantaJob job) throws IOException {
        final Create newJob = new Create(this, job.getName());
        newJob.addPhases(job.getPhases());

        try (Stream<String> inputs = client.getJobInputs(job.getId())) {
            newJob.addInputs(inputs);
            return newJob;
        }
    }

    /**
     * Inner class that provides methods used in the construction of a new job.
     * This allows us to easily add inputs and phases to the job.
     */
    public static class Create {
        /**
         * Name of job.
         */
        private final String name;

        /**
         * Reference to {@link MantaJobBuilder} instance that created this instance.
         */
        private final MantaJobBuilder parent;

        /**
         * A list of the phases used in the job.
         */
        private final List<MantaJobPhase> phases = new CopyOnWriteArrayList<>();

        /**
         * A list of the inputs to be attached to the job.
         */
        private final List<String> inputs = new CopyOnWriteArrayList<>();


        /**
         * Creates a new instance of the fluent job creation class.
         *
         * @param parent reference to {@link MantaJobBuilder} instance that created this instance
         * @param name name of the job
         */
        private Create(final MantaJobBuilder parent, final String name) {
            this.parent = parent;
            this.name = name;
        }

        /**
         * Add a single phase to the job.
         *
         * @param phase phase to add
         * @return reference to the create fluent builder
         */
        public Create addPhase(final MantaJobPhase phase) {
            Validate.notNull(phase, "Phase must not be null");
            phases.add(phase);
            return this;
        }

        /**
         * Add multiple phases to the job.
         *
         * @param additionalPhases phases to add
         * @return reference to the create fluent builder
         */
        @SuppressWarnings("UnusedReturnValue")
        public Create addPhases(final Iterable<MantaJobPhase> additionalPhases) {
            Validate.notNull(phases, "Phases must not be null");

            for (MantaJobPhase phase : additionalPhases) {
                phases.add(phase);
            }

            return this;
        }

        /**
         * Add multiple phases to the job.
         *
         * @param additionalPhases phases to add
         * @return reference to the create fluent builder
         */
        public Create addPhases(final Stream<MantaJobPhase> additionalPhases) {
            Validate.notNull(phases, "Phases must not be null");

            additionalPhases.forEach(phases::add);

            return this;
        }

        /**
         * Add multiple phases to the job.
         *
         * @param additionalPhases phases to add
         * @return reference to the create fluent builder
         */
        public Create addPhases(final MantaJobPhase... additionalPhases) {
            Validate.notNull(phases, "Phases must not be null");

            Collections.addAll(phases, additionalPhases);

            return this;
        }

        /**
         * Add input to job.
         *
         * @param input path to Manta object representing the input.
         * @return reference to the create fluent builder
         */
        public Create addInput(final String input) {
            inputs.add(input);

            return this;
        }

        /**
         * Adds multiple inputs to the job.
         *
         * @param additionalInputs multiple inputs to add
         * @return reference to the create fluent builder
         */
        public Create addInputs(final Iterable<String> additionalInputs) {
            for (String input : additionalInputs) {
                inputs.add(input);
            }

            return this;
        }

        /**
         * Adds multiple inputs to the job.
         *
         * @param additionalInputs multiple inputs to add
         * @return reference to the create fluent builder
         */
        @SuppressWarnings("UnusedReturnValue")
        public Create addInputs(final Stream<String> additionalInputs) {
            additionalInputs.forEach(inputs::add);

            return this;
        }

        /**
         * Adds multiple inputs to the job.
         *
         * @param additionalInputs multiple inputs to add
         * @return reference to the create fluent builder
         */
        public Create addInputs(final String... additionalInputs) {
            Collections.addAll(inputs, additionalInputs);

            return this;
        }

        /**
         * Validates the inputs already added and throws a {@link MantaJobException}
         * if any of the inputs are inaccessible.
         *
         * @return reference to the create fluent builder
         * @throws MantaJobException thrown if any of the inputs were invalid
         */
        public Create validateInputs() {
            StringBuilder builder = new StringBuilder();
            Map<String, Exception> generalExceptions = new HashMap<>();
            Map<String, MantaClientHttpResponseException> mantaExceptions = new HashMap<>();

            for (String input : inputs) {
                try {
                    MantaObject obj = parent.client.head(input);

                    if (obj.isDirectory()) {
                        throw new MantaException("Object is not a file, but is a directory");
                    }

                } catch (MantaClientHttpResponseException e) {
                    mantaExceptions.put(input, e);
                } catch (Exception e) {
                    generalExceptions.put(input, e);
                }
            }

            if (!generalExceptions.isEmpty() || !mantaExceptions.isEmpty()) {
                builder.append("Error with inputs:");

                generalExceptions.entrySet().forEach(entry ->
                        builder.append(System.lineSeparator())
                       .append("[").append(entry.getKey()).append("] ")
                       .append(entry.getValue().getMessage()));

                mantaExceptions.entrySet().forEach(entry ->
                        builder.append(System.lineSeparator())
                        .append("[").append(entry.getKey()).append("] ")
                        .append("(").append(entry.getValue().getServerCode()).append(") ")
                        .append(entry.getValue().getMessage()));

                throw new MantaJobException(builder.toString());
            }

            return this;
        }

        /**
         * Run the created job by invoking the remote Manta API.
         *
         * @return reference to the create fluent builder
         * @throws IOException thrown when we are unable to execute the job
         */
        public Run run() throws IOException {
            final MantaJob job = new MantaJob(name, phases);
            final UUID id = parent.client.createJob(job);
            parent.client.addJobInputs(id, inputs.iterator());

            if (!parent.client.endJobInput(id)) {
                throw new MantaJobException(id, "Unable to end inputs to job");
            }

            return new Run(parent, id);
        }
    }

    /**
     * Inner class used when a job is at its running stage or it has finished
     * running but we don't know if it has finished yet. This class provides
     * methods that allow us to check on the state of the job or to wait until
     * the job is done.
     */
    public static class Run {
        /**
         * Job ID.
         */
        private final UUID id;

        /**
         * Reference to {@link MantaJobBuilder} instance that created this instance.
         */
        private final MantaJobBuilder parent;

        /**
         * Default wait time between checks to the Manta API.
         */
        private static final Duration DEFAULT_TIME_BETWEEN_POLLS =
                Duration.of(3L, ChronoUnit.SECONDS);
        /**
         * Default maximum times to check the Manta API to see if a job has finished.
         */
        private static final int DEFAULT_MAX_POLLS = 100;


        /**
         * Creates an instance of the inner class used for providing a fluent
         * API for jobs after they have been started.
         *
         * @param parent reference to {@link MantaJobBuilder} instance that created this instance
         * @param id job id
         */
        Run(final MantaJobBuilder parent, final UUID id) {
            this.parent = parent;
            this.id = id;
        }

        /**
         * Cancel the job.
         *
         * @throws IOException thrown when there is a problem canceling a job over the network
         */
        public void cancel() throws IOException {
            parent.client.cancelJob(id);
        }

        /**
         * Check the Manta API to see if a Job is done.
         * @return true if the job is done otherwise false
         * @throws IOException thrown if there is a problem getting status over the network
         */
        public boolean isDone() throws IOException {
            return getJob().getState().equals("done");
        }

        /**
         * Get the current job's state represented as a {@link MantaJob} object.
         *
         * @return object containing the job's state
         * @throws IOException thrown when there is a problem getting the job info over the network
         */
        public MantaJob getJob() throws IOException {
            return parent.client.getJob(id);
        }

        /**
         * Get the current job's id.
         *
         * @return job id of the current run
         */
        public UUID getId() {
            return this.id;
        }

        /**
         * Wait for the job to finish. We wait for a pre-set amount of time then
         * poll the remote API. If the job is finished we return.
         *
         * @return object providing a fluent interface for finished jobs
         * @throws MantaJobException thrown if we have waited too long for a job to complete
         * @throws IOException  thrown if there is a problem getting the job's status over the network
         */
        public Done waitUntilDone() throws IOException {
            return waitUntilDone(DEFAULT_TIME_BETWEEN_POLLS, DEFAULT_MAX_POLLS);
        }

        /**
         * Wait for the job to finish. We wait for the specified amount of time then
         * poll the remote API the specified amount of times. If the job is finished we
         * return.
         *
         * @param timeBetweenPolls time to wait between polls to the Manta API
         * @param maxPolls maximum times to poll the Manta API before giving up
         * @return object providing a fluent interface for finished jobs
         * @throws MantaJobException thrown if we have waited too long for a job to complete
         * @throws IOException  thrown if there is a problem getting the job's status over the network
         */
        public Done waitUntilDone(final Duration timeBetweenPolls, final int maxPolls)
                throws IOException {

            for (int i = 0; i < maxPolls; i++) {
                if (isDone()) {
                    return new Done(this.id, this.parent);
                }

                try {
                    LOG.debug("Waiting for {} ms before checking again. Check #{}",
                            timeBetweenPolls.toMillis(), i);
                    Thread.sleep(timeBetweenPolls.toMillis());
                } catch (InterruptedException e) {
                    throw new MantaJobException(id, "Can't wait any longer for job to finish", e);
                }
            }

            throw new MantaJobException(id, "Job didn't complete in the expected amount of time");
        }

        /**
         * Go to the done phase of this job without checking to see if the job
         * is done. Use this at your own risk.
         *
         * @return object providing a fluent interface for finished jobs
         */
        public Done done() {
            return new Done(this.id, this.parent);
        }
    }

    /**
     * Inner class that provides methods that allow us to interact with a job
     * that has finished. For example, this would allow us to get the outputs
     * or the errors from a job.
     */
    public static class Done {
        /**
         * Job ID.
         */
        private final UUID id;

        /**
         * Reference to {@link MantaJobBuilder} instance that created this instance.
         */
        private final MantaJobBuilder parent;

        /**
         * Creates an instance of the inner class used for providing a fluent
         * API for jobs after they have finished.
         *
         * @param parent reference to {@link MantaJobBuilder} instance that created this instance
         * @param id job id
         */
        Done(final UUID id, final MantaJobBuilder parent) {
            this.id = id;
            this.parent = parent;
        }

        /**
         * <p>Returns a stream of {@link InputStream} implementations for each
         * output returned from the Manta API for the job.</p>
         *
         * <p><strong>Make sure to close this stream when you are done with
         * otherwise the HTTP socket will remain open.</strong></p>
         * @return stream of each output's input stream
         * @throws IOException thrown when we can't get a list of outputs over the network
         */
        public Stream<MantaObjectInputStream> outputAsStreams() throws IOException {
            return parent.client.getJobOutputsAsStreams(id);
        }

        /**
         * <p>Returns a stream of strings containing all of the
         * output returned from the Manta API for a job. Be careful, this method
         * is not memory-efficient.</p>
         *
         * <p><strong>Make sure to close this stream when you are done with
         * otherwise the HTTP socket will remain open.</strong></p>
         * @return stream of each job output as a string
         * @throws IOException thrown when we can't get a list of outputs over the network
         */
        public Stream<String> outputs() throws IOException {
            return parent.client.getJobOutputsAsStrings(id);
        }

        /**
         * <p>Returns a list of failure details for each object in which a failure
         * occurred.</p>
         *
         * <p><strong>Make sure to close this stream when you are done with
         * otherwise the HTTP socket will remain open.</strong></p>
         *
         * @return a stream of job error objects
         * @throws IOException thrown when we can't get a list of errors over the network
         */
        public Stream<MantaJobError> errors() throws IOException {
            return parent.client.getJobErrors(id);
        }

        /**
         * Get the current job's state represented as a {@link MantaJob} object.
         *
         * @return object containing the job's state
         * @throws IOException thrown when there is a problem getting the job info over the network
         */
        public MantaJob getJob() throws IOException {
            return parent.client.getJob(id);
        }

        /**
         * Test to see if the job failed.
         * @return true if any of the inputs failed, otherwise false
         * @throws IOException thrown if we can't get the status over the network
         */
        public boolean failed() throws IOException {
            final Number errors = getJob().getStats().get("errors");

            if (errors != null) {
                return errors.intValue() > 0;
            } else {
                throw new MantaClientException("Unable to get error stats");
            }
        }

        /**
         * Test to see if the job was successful.
         *
         * @return true if non of the inputs had errors, otherwise false
         * @throws IOException thrown if we can't get the status over the network
         */
        public boolean successful() throws IOException {
            return !failed();
        }

        /**
         * Validate that all of the inputs completed without errors.
         * If there was an error(s), we compile all of the errors into as
         * a property on {@link MantaJobException}.
         *
         * @return reference to the current Done object
         * @throws IOException thrown if we have trouble getting status over the network
         * @throws MantaJobException thrown if there was a failure processing the job
         */
        public Done validateJobsSucceeded() throws IOException {
            if (successful()) {
                return this;
            }

            throw new MantaJobException(id,
                    errors().collect(Collectors.toList()));
        }
    }
}
