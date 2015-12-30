package com.joyent.manta;

import com.joyent.manta.client.MantaClient;
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
            setupTestDirectory();
            String path = addTestFile(FileUtils.ONE_KB * 128);

            final int iterations = 10;
            long aggregation = 0;

            for (int i = 0; i < iterations; i++) {
                Duration duration = measureGet(path);
                long millis = duration.toMillis();
                aggregation += millis;
                System.out.printf("%s %s\n", path, millis);
            }

            final long average = Math.round(aggregation / iterations);
            System.out.printf("Average latency: %d\n", average);

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
            client.deleteRecursive(testDirectory);
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

    private static Duration measureGet(final String path) throws IOException {
        final Instant start = Instant.now();
        try (InputStream is = client.getAsInputStream(path)) {
            while (is.read() != -1);
        }
        final Instant stop = Instant.now();

        return Duration.between(start, stop);
    }
}
