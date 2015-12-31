package com.joyent.manta;

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
public class Benchmark {
    private static Logger LOG = LoggerFactory.getLogger(Benchmark.class);
    private static ConfigContext config;
    private static MantaClient client;
    private static UUID testRunId = UUID.randomUUID();
    private static String testDirectory;

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
                sizeInKb = 128;
            }

            final int iterations;
            if (argv.length > 1) {
                iterations = Integer.parseInt(argv[1]);
            } else {
                iterations = 10;
            }

            System.out.printf("Testing latencies on a %d kb object for %d iterations\n",
                    sizeInKb, iterations);

            setupTestDirectory();
            String path = addTestFile(FileUtils.ONE_KB * sizeInKb);

            long fullAggregation = 0;
            long serverAggregation = 0;

            for (int i = 0; i < iterations; i++) {
                Duration[] durations = measureGet(path);
                long fullLatency = durations[0].toMillis();
                long serverLatency = durations[1].toMillis();
                fullAggregation += fullLatency;
                serverAggregation += serverLatency;

                System.out.printf("Read %d full=%dms, server=%dms\n",
                        i, fullLatency, serverLatency);
            }

            final long fullAverage = Math.round(fullAggregation / iterations);
            final long serverAverage = Math.round(serverAggregation / iterations);
            System.out.printf("Average full latency: %d ms\n", fullAverage);
            System.out.printf("Average server latency: %d ms\n", serverAverage);

        } catch (IOException e) {
            LOG.error("Error running benchmark", e);
        } finally {
            cleanUp();
            client.closeQuietly();
        }
    }

    private static void setupTestDirectory() throws IOException {
        client.putDirectory(testDirectory);
    }

    private static void cleanUp() {
        try {
//            client.deleteRecursive(testDirectory);
        } catch (Exception e) {
            LOG.error("Error cleaning up benchmark", e);
        }
    }

    private static String addTestFile(final long size) throws IOException {
        try (InputStream is = new RandomInputStream(size)) {
            String path = String.format("%s/%s.random", testDirectory,
                    UUID.randomUUID());
            client.put(path, is);
            return path;
        }
    }

    private static Duration[] measureGet(final String path) throws IOException {
        final Instant start = Instant.now();
        final String serverLatencyString;
                try (MantaObjectInputStream is = client.getAsInputStream(path)) {
            while (is.skip(1024) != 0);
            serverLatencyString = ((ArrayList<?>)is.getHeader("x-response-time")).get(0).toString();
        }
        final Instant stop = Instant.now();

        Duration serverLatency = Duration.ofMillis(Long.parseLong(serverLatencyString));
        Duration fullLatency = Duration.between(start, stop);
        return new Duration[] { fullLatency, serverLatency };
    }
}
