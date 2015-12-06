/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaJobException;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobBuilder {
    private final MantaClient client;

    MantaJobBuilder(final MantaClient client) {
        this.client = client;
    }

    public Create newJob(final String name) {
        return new Create(this, name);
    }

    public static class Create {
        private final String name;
        private final MantaJobBuilder parent;
        private final List<MantaJobPhase> phases = new CopyOnWriteArrayList<>();
        private final List<String> inputs = new CopyOnWriteArrayList<>();

        private Create(final MantaJobBuilder parent, final String name) {
            this.parent = parent;
            this.name = name;
        }

        public Create addPhase(final MantaJobPhase phase) {
            phases.add(phase);
            return this;
        }

        public Create addPhases(final Iterable<MantaJobPhase> additionalPhases) {
            for (MantaJobPhase phase : additionalPhases) {
                phases.add(phase);
            }

            return this;
        }

        public Create addPhases(final Stream<MantaJobPhase> additionalPhases) {
            additionalPhases.forEach(phases::add);

            return this;
        }

        public Create addInput(final String input) {
            inputs.add(input);

            return this;
        }

        public Create addInputs(final Iterable<String> additionalInputs) {
            for (String input : additionalInputs) {
                inputs.add(input);
            }

            return this;
        }

        public Create addInputs(final Stream<String> additionalInputs) {
            additionalInputs.forEach(inputs::add);

            return this;
        }

        public Run run() throws IOException {
            final MantaJob job = new MantaJob(name, phases);
            final UUID id = parent.client.createJob(job);
            parent.client.addJobInputs(id, inputs.iterator());
            parent.client.endJobInput(id);

            return new Run(parent, id);
        }
    }

    public static class Run {
        private final UUID id;
        private final MantaJobBuilder parent;
        private static final Duration DEFAULT_TIME_BETWEEN_POLLS =
                Duration.of(3L, ChronoUnit.SECONDS);

        private static final int DEFAULT_MAX_POLLS = 100;

        Run(final MantaJobBuilder parent, final UUID id) {
            this.parent = parent;
            this.id = id;
        }

        public void cancel() throws IOException {
            parent.client.cancelJob(id);
        }

        public boolean isDone() throws IOException {
            return getJob().getState().equals("done");
        }

        public MantaJob getJob() throws IOException {
            return parent.client.getJob(id);
        }

        public Done waitUntilDone() throws IOException {
            return waitUntilDone(DEFAULT_TIME_BETWEEN_POLLS, DEFAULT_MAX_POLLS);
        }

        public Done waitUntilDone(final Duration timeBetweenPolls, final int maxPolls)
                throws IOException {

            for (int i = 0; i < maxPolls; i++) {
                if (isDone()) {
                    return new Done(this.id, this.parent);
                }

                try {
                    wait(timeBetweenPolls.toMillis());
                } catch (InterruptedException e) {
                    throw new IOException("Can't wait any longer for job to finish", e);
                }
            }

            throw new MantaClientException("Job didn't complete in the expected amount of time");
        }
    }

    public static class Done {
        private final UUID id;
        private final MantaJobBuilder parent;

        Done(UUID id, MantaJobBuilder parent) {
            this.id = id;
            this.parent = parent;
        }

        public Stream<MantaObjectInputStream> outputAsStreams() throws IOException {
            return parent.client.getJobOutputsAsStreams(id);
        }

        public Stream<String> outputs() throws IOException {
            return parent.client.getJobOutputsAsStrings(id);
        }

        public Stream<MantaJobError> errors() throws IOException {
            return parent.client.getJobErrors(id);
        }

        public MantaJob getJob() throws IOException {
            return parent.client.getJob(id);
        }

        public boolean failed() throws IOException {
            final Number errors = getJob().getStats().get("errors");

            if (errors != null) {
                return errors.intValue() > 0;
            } else {
                throw new MantaClientException("Unable to get error stats");
            }
        }

        public boolean successful() throws IOException {
            return !failed();
        }

        public Done throwExceptionIfFailed() throws IOException {
            if (successful()) {
                return this;
            }

            throw new MantaJobException(errors().collect(Collectors.toList()));
        }
    }
}
