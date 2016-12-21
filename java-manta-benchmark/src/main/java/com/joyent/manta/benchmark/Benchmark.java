/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.benchmark;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObjectInputStream;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
     * Number of bytes to skip at one time when looping over streams.
     */
    private static final int SKIP_VALUE = 1024;

    /**
     * Configuration context that informs the Manta client about its settings.
     */
    private static ConfigContext config;

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
     * Use the main method and not the constructor.
     */
    private Benchmark() {
    }

    /**
     * Entrance to benchmark utility.
     * @param argv first param is the size of object in kb, second param is the number of iterations
     * @throws Exception when something goes wrong
     */
    public static void main(final String[] argv) throws Exception {
        config = new ChainedConfigContext(
                new DefaultsConfigContext(),
                new SystemSettingsConfigContext()
        );
        client = new MantaClient(config);
        testDirectory = String.format("%s/stor/benchmark-%s",
                config.getMantaHomeDirectory(), testRunId);

        try {
            final long sizeInKb;
            if (argv.length > 0) {
                sizeInKb = Long.parseLong(argv[0]);
            } else {
                sizeInKb = DEFAULT_OBJ_SIZE_KB;
            }

            final int iterations;
            if (argv.length > 1) {
                iterations = Integer.parseInt(argv[1]);
            } else {
                iterations = DEFAULT_ITERATIONS;
            }

            final int concurrency;
            if (argv.length > 2) {
                concurrency = Integer.parseInt(argv[2]);
            } else {
                concurrency = DEFAULT_CONCURRENCY;
            }

            System.out.printf("Testing latencies on a %d kb object for %d "
                            + "iterations with a concurrency value of %d\n",
                    sizeInKb, iterations, concurrency);

            setupTestDirectory();
            String path = addTestFile(FileUtils.ONE_KB * sizeInKb);

            if (concurrency == 1) {
                singleThreadedBenchmark(path, iterations);
            } else {
                multithreadedBenchmark(path, iterations, concurrency);
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
     * @param path path to store benchmarking test data
     * @param iterations number of iterations to run
     * @throws IOException thrown when we can't communicate with the server
     */
    private static void singleThreadedBenchmark(final String path,
                                                final int iterations) throws IOException {
        long fullAggregation = 0;
        long serverAggregation = 0;

        final long testStart = System.currentTimeMillis();

        for (int i = 0; i < iterations; i++) {
            Duration[] durations = measureGet(path);
            long fullLatency = durations[0].toMillis();
            long serverLatency = durations[1].toMillis();
            fullAggregation += fullLatency;
            serverAggregation += serverLatency;

            System.out.printf("Read %d full=%dms, server=%dms\n",
                    i, fullLatency, serverLatency);
        }

        final long testEnd = System.currentTimeMillis();

        final long fullAverage = Math.round(fullAggregation / iterations);
        final long serverAverage = Math.round(serverAggregation / iterations);
        final long totalTime = testEnd - testStart;

        System.out.printf("Average full latency: %d ms\n", fullAverage);
        System.out.printf("Average server latency: %d ms\n", serverAverage);
        System.out.printf("Total test time: %d ms\n", totalTime);
    }

    /**
     * Method used to run a multi-threaded benchmark.
     *
     * @param path path to store benchmarking test data
     * @param iterations number of iterations to run
     * @param concurrency number of threads to run
     * @throws IOException thrown when we can't communicate with the server
     */
    private static void multithreadedBenchmark(final String path,
                                               final int iterations,
                                               final int concurrency) throws IOException {
        final AtomicLong fullAggregation = new AtomicLong(0L);
        final AtomicLong serverAggregation = new AtomicLong(0L);
        final AtomicLong count = new AtomicLong(0L);
        final CountDownLatch latch = new CountDownLatch(concurrency);
        final long testStart = System.currentTimeMillis();

        final Callable<Void> worker = () -> {
            long i;

            while ((i = count.incrementAndGet()) <= iterations) {
                Duration[] durations = measureGet(path);
                long fullLatency = durations[0].toMillis();
                long serverLatency = durations[1].toMillis();
                fullAggregation.addAndGet(fullLatency);
                serverAggregation.addAndGet(serverLatency);

                System.out.printf("Read %d full=%dms, server=%dms\n",
                        i, fullLatency, serverLatency);
            }

            latch.countDown();
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
            executor.invokeAll(workers);
            latch.await();
        } catch (InterruptedException e) {
            return;
        } finally {
            executor.shutdown();
        }


        final long testEnd = System.currentTimeMillis();

        final long fullAverage = Math.round(fullAggregation.get() / iterations);
        final long serverAverage = Math.round(serverAggregation.get() / iterations);
        final long totalTime = testEnd - testStart;

        System.out.printf("Average full latency: %d ms\n", fullAverage);
        System.out.printf("Average server latency: %d ms\n", serverAverage);
        System.out.printf("Total test time: %d ms\n", totalTime);
        System.out.printf("Total invocations: %d\n", count.get());
    }

    /**
     * Creates test directory.
     *
     * @throws IOException thrown when we can't access Manta over the network
     */
    private static void setupTestDirectory() throws IOException {
        client.putDirectory(testDirectory);
    }

    /**
     * Cleans up the test directory.
     */
    private static void cleanUp() {
        try {
            client.deleteRecursive(testDirectory);
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
            while (is.skip(SKIP_VALUE) != 0) { }
            serverLatencyString = is.getHeader("x-response-time").toString();
        }
        final Instant stop = Instant.now();

        Duration serverLatency = Duration.ofMillis(Long.parseLong(serverLatencyString));
        Duration fullLatency = Duration.between(start, stop);
        return new Duration[] {fullLatency, serverLatency};
    }
}
