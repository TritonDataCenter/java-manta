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
import java.util.UUID;

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

            System.out.printf("Testing latencies on a %d kb object for %d iterations\n",
                    sizeInKb, iterations);

            setupTestDirectory();
            String path = addTestFile(FileUtils.ONE_KB * sizeInKb);

            long fullAggregation = 0;
            long serverAggregation = 0;

            long testStart = System.currentTimeMillis();

            for (int i = 0; i < iterations; i++) {
                Duration[] durations = measureGet(path);
                long fullLatency = durations[0].toMillis();
                long serverLatency = durations[1].toMillis();
                fullAggregation += fullLatency;
                serverAggregation += serverLatency;

                System.out.printf("Read %d full=%dms, server=%dms\n",
                        i, fullLatency, serverLatency);
            }

            long testEnd = System.currentTimeMillis();

            final long fullAverage = Math.round(fullAggregation / iterations);
            final long serverAverage = Math.round(serverAggregation / iterations);
            final long totalTime = testEnd - testStart;

            System.out.printf("Average full latency: %d ms\n", fullAverage);
            System.out.printf("Average server latency: %d ms\n", serverAverage);
            System.out.printf("Total test time: %d ms\n", totalTime);
        } catch (IOException e) {
            LOG.error("Error running benchmark", e);
        } finally {
            cleanUp();
            client.closeQuietly();
        }
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
            serverLatencyString = ((ArrayList<?>)is.getHeader("x-response-time")).get(0).toString();
        }
        final Instant stop = Instant.now();

        Duration serverLatency = Duration.ofMillis(Long.parseLong(serverLatencyString));
        Duration fullLatency = Duration.between(start, stop);
        return new Duration[] {fullLatency, serverLatency};
    }
}
