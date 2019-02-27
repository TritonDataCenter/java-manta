/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.benchmark;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.text.CharacterPredicates;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Benchmark class that can be invoked to get some simple benchmarks about
 * Manta performance from the command line.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public final class Benchmark {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(Benchmark.class);

    /**
     * Default object size.
     */
    private static final int DEFAULT_OBJ_SIZE_KB = 128;

    /**
     * Default number of iterations.
     */
    private static final int DEFAULT_ITERATIONS = 10;

    /**
     * Default number of threads to concurrently run.
     */
    private static final int DEFAULT_CONCURRENCY = 1;

    /**
     * Time to wait until checking to see if a thread pool has finished.
     */
    private static final long CHECK_INTERVAL = Duration.ofSeconds(1).getSeconds();

    /**
     * Manta client library.
     */
    private static MantaClient client;

    /**
     * Unique test run id.
     */
    private static UUID testRunId = UUID.randomUUID();

    /**
     * Test directory.
     */
    private static String testDirectory;

    /**
     * Size of object in bytes or number of directories.
     */
    private static int sizeInBytesOrNoOfDirs;

    /**
     * Random string generator instance for generating test data.
     */
    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .filteredBy(CharacterPredicates.LETTERS)
                    .build();

    /**
     * Use the main method and not the constructor.
     */
    private Benchmark() {
    }

    /**
     * Entrance to benchmark utility.
     * @param argv param1: method, param2: size of object in kb, param3: no of iterations, param4: threads
     */
    public static void main(final String[] argv) {
        // Configuration context that informs the Manta client about its settings.
        final ConfigContext config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new SystemSettingsConfigContext()
        );

        client = new MantaClient(config);
        testDirectory = String.format("%s/stor/java-manta-integration-tests/benchmark-%s",
                config.getMantaHomeDirectory(), testRunId);

        if (argv.length == 0) {
            System.err.println("Benchmark requires the following parameters:\n"
            + "method, size of object in kb, number of iterations, concurrency");
        }

        String method = argv[0];

        try {
            if (argv.length > 1) {
                sizeInBytesOrNoOfDirs = Integer.parseInt(argv[1]);
            } else {
                sizeInBytesOrNoOfDirs = DEFAULT_OBJ_SIZE_KB;
            }

            final int iterations;
            if (argv.length > 2) {
                iterations = Integer.parseInt(argv[2]);
            } else {
                iterations = DEFAULT_ITERATIONS;
            }

            final int concurrency;
            if (argv.length > 3) {
                concurrency = Integer.parseInt(argv[3]);
            } else {
                concurrency = DEFAULT_CONCURRENCY;
            }

            final long actualIterations = perThreadCount(iterations, concurrency) * concurrency;

            System.out.printf("Testing latencies on a %d kb object for %d "
                            + "iterations with a concurrency value of %d\n",
                    sizeInBytesOrNoOfDirs, actualIterations, concurrency);

            setupTestDirectory();
            String path = addTestFile(FileUtils.ONE_KB * sizeInBytesOrNoOfDirs);

            if (concurrency == 1) {
                singleThreadedBenchmark(method, path, iterations);
            } else {
                multithreadedBenchmark(method, path, iterations, concurrency);
            }
        } catch (IOException e) {
            LOG.error("Error running benchmark", e);
        } finally {
            cleanUp();
            client.closeWithWarning();
        }
    }

    /**
     * Method used to run a simple single-threaded benchmark.
     *
     * @param method to measure
     * @param path path to store benchmarking test data
     * @param iterations number of iterations to run
     * @throws IOException thrown when we can't communicate with the server
     */
    private static void singleThreadedBenchmark(final String method,
                                                final String path,
                                                final int iterations) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(Benchmark::cleanUp));

        long fullAggregation = 0;
        long serverAggregation = 0;

        final long testStart = System.nanoTime();

        for (int i = 0; i < iterations; i++) {
            Duration[] durations;

            if (method.equals("put")) {
                durations = measurePut(sizeInBytesOrNoOfDirs);
            } else if (method.equals("putDir")) {
                durations = measurePutDir(sizeInBytesOrNoOfDirs);
            } else {
                durations = measureGet(path);
            }

            long fullLatency = durations[0].toMillis();
            long serverLatency = durations[1].toMillis();
            fullAggregation += fullLatency;
            serverAggregation += serverLatency;

            System.out.printf("%s %d full=%dms, server=%dms\n",
                    method, i, fullLatency, serverLatency);
        }

        final long testEnd = System.nanoTime();

        final long fullAverage = new BigDecimal(fullAggregation)
                .divide(new BigDecimal(iterations), RoundingMode.HALF_UP).longValue();
        final long serverAverage = new BigDecimal(serverAggregation)
                .divide(new BigDecimal(iterations), RoundingMode.HALF_UP).longValue();
        final long totalTime = testEnd - testStart;

        System.out.printf("Average full latency: %d ms\n", fullAverage);
        System.out.printf("Average server latency: %d ms\n", serverAverage);
        System.out.printf("Total test time: %d ms\n",
                Duration.ofNanos(totalTime).toMillis());
    }

    /**
     * Method used to run a multi-threaded benchmark.
     *
     * @param method to measure
     * @param path path to store benchmarking test data
     * @param iterations number of iterations to run
     * @param concurrency number of threads to run
     */
    private static void multithreadedBenchmark(final String method,
                                               final String path,
                                               final int iterations,
                                               final int concurrency) {
        final AtomicLong fullAggregation = new AtomicLong(0L);
        final AtomicLong serverAggregation = new AtomicLong(0L);
        final AtomicLong count = new AtomicLong(0L);
        final long perThreadCount = perThreadCount(iterations, concurrency);

        System.out.printf("Running %d iterations per thread\n", perThreadCount);

        final long testStart = System.nanoTime();

        Runtime.getRuntime().addShutdownHook(new Thread(Benchmark::cleanUp));

        final Callable<Void> worker = () -> {
            for (int i = 0; i < perThreadCount; i++) {
                Duration[] durations;

                if (method.equals("put")) {
                    durations = measurePut(sizeInBytesOrNoOfDirs);
                } else if (method.equals("putDir")) {
                    durations = measurePutDir(sizeInBytesOrNoOfDirs);
                } else {
                    durations = measureGet(path);
                }

                long fullLatency = durations[0].toMillis();
                long serverLatency = durations[1].toMillis();
                fullAggregation.addAndGet(fullLatency);
                serverAggregation.addAndGet(serverLatency);

                System.out.printf("%s %d full=%dms, server=%dms, thread=%s\n",
                        method, count.getAndIncrement(), fullLatency, serverLatency,
                        Thread.currentThread().getName());
            }

            return null;
        };

        final Thread.UncaughtExceptionHandler handler = (t, e) ->
                LOG.error("Error when executing benchmark", e);

        final AtomicInteger threadCounter = new AtomicInteger(0);
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(handler);
            t.setName(String.format("benchmark-%d", threadCounter.incrementAndGet()));

            return t;
        };

        ExecutorService executor = Executors.newFixedThreadPool(concurrency, threadFactory);

        List<Callable<Void>> workers = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            workers.add(worker);
        }

        try {
            List<Future<Void>> futures = executor.invokeAll(workers);

            boolean completed = false;
            while (!completed) {
                try (Stream<Future<Void>> stream = futures.stream()) {
                    completed = stream.allMatch((f) -> f.isDone() || f.isCancelled());

                    if (!completed) {
                        Thread.sleep(CHECK_INTERVAL);
                    }
                }
            }

        } catch (InterruptedException e) {
            return;
        } finally {
            System.err.println("Shutting down the thread pool");
            executor.shutdown();
        }

        final long testEnd = System.nanoTime();

        final long fullAverage = new BigDecimal(fullAggregation.get())
                .divide(new BigDecimal(iterations), RoundingMode.HALF_UP).longValue();
        final long serverAverage = new BigDecimal(serverAggregation.get())
                .divide(new BigDecimal(iterations), RoundingMode.HALF_UP).longValue();
        final long totalTime = Duration.ofNanos(testEnd - testStart).toMillis();

        System.out.printf("Average full latency: %d ms\n", fullAverage);
        System.out.printf("Average server latency: %d ms\n", serverAverage);
        System.out.printf("Total test time: %d ms\n", totalTime);
        System.out.printf("Total invocations: %d\n", count.get());
    }

    /**
     * Calculates the number of iterations to run per thread.
     *
     * @param iterations number of iterations to run
     * @param concurrency number of threads to run
     * @return iterations / concurrency properly rounded
     */
    private static long perThreadCount(final int iterations,
                                       final int concurrency) {
        return (long) Math.floorDiv(iterations, concurrency);
    }

    /**
     * Creates test directory.
     *
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static void setupTestDirectory() throws IOException {
        client.putDirectory(testDirectory, true);
    }

    /**
     * Cleans up the test directory.
     */
    private static void cleanUp() {
        try {
            if (!client.isClosed()) {
                System.out.printf("Attempting to clean up remote files in %s\n",
                        testDirectory);

                client.deleteRecursive(testDirectory);
                client.closeWithWarning();
            }
        } catch (Exception e) {
            LOG.error("Error cleaning up benchmark", e);
        }
    }

    /**
     * Adds a file (object) for testing.
     *
     * @param size size of object to add
     * @return path to the object added
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static String addTestFile(final long size) throws IOException {
        try (InputStream is = new RandomInputStream(size)) {
            String path = String.format("%s/%s.random", testDirectory,
                    UUID.randomUUID());
            client.put(path, is);
            return path;
        }
    }

    /**
     * Measures the total time to get an object from Manta.
     *
     * @param path path of the object to measure
     * @return two durations - full time in the JVM, server time processing
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static Duration[] measureGet(final String path) throws IOException {
        final Instant start = Instant.now();
        final String serverLatencyString;


        try (MantaObjectInputStream is = client.getAsInputStream(path)) {
            copyToTheEther(is);
            serverLatencyString = is.getHeader("x-response-time").toString();
        }

        final Instant stop = Instant.now();

        Duration serverLatency = Duration.ofMillis(Long.parseLong(serverLatencyString));
        Duration fullLatency = Duration.between(start, stop);
        return new Duration[] {fullLatency, serverLatency};
    }

    /**
     * Copies the entirety of an input stream to a {@link NullOutputStream}.
     * @param input stream to copy
     * @throws IOException thrown when you can't copy to nothing
     */
    @SuppressWarnings("InnerAssignment")
    private static void copyToTheEther(final InputStream input) throws IOException {

        try (OutputStream output = new NullOutputStream()) {
            final byte[] buffer = new byte[512];

            for (int n; -1 != (n = input.read(buffer));) {
                output.write(buffer, 0, n);
            }
        }
    }

    /**
     * Measures the total time to put an object to Manta.
     *
     * @param length number of bytes to write
     * @return two durations - full time in the JVM, server time processing
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static Duration[] measurePut(final long length) throws IOException {
        final String path = String.format("%s/%s", testDirectory,
                UUID.randomUUID());
        final long start = System.nanoTime();
        final String serverLatencyString;

        try (RandomInputStream rand = new RandomInputStream(length)) {
            MantaHttpHeaders headers = new MantaHttpHeaders();
            headers.setDurabilityLevel(2);

            serverLatencyString = client.put(path, rand, length, headers, null)
                    .getHeader("x-response-time").toString();
        }

        final long stop = System.nanoTime();

        Duration serverLatency = Duration.ofMillis(Long.parseLong(serverLatencyString));
        Duration fullLatency = Duration.ofNanos(stop - start);
        return new Duration[] {fullLatency, serverLatency};
    }

    /**
     * Measures the total time to put multiple directories to Manta.
     *
     * @param diretoryCount number of directories to create
     * @return two durations - full time in the JVM, -1 because server time is unavailable
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static Duration[] measurePutDir(final int diretoryCount) throws IOException {
        final StringBuilder path = new StringBuilder()
                .append(testDirectory);

        for (int i = 0; i < diretoryCount; i++) {
            path.append(MantaClient.SEPARATOR)
                .append(STRING_GENERATOR.generate(2));
        }

        final long start = System.nanoTime();

        client.putDirectory(path.toString(), true);

        final long stop = System.nanoTime();

        Duration serverLatency = Duration.ofMillis(-1L);
        Duration fullLatency = Duration.ofNanos(stop - start);
        return new Duration[] {fullLatency, serverLatency};
    }
}
