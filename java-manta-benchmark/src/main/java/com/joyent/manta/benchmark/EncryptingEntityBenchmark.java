package com.joyent.manta.benchmark;

import com.joyent.manta.client.crypto.EncryptingEntity;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.http.entity.DigestedEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.io.output.NullOutputStream;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.time.Duration;

/**
 * Benchmark for determining the throughput of an encryption algorithm.
 */
public final class EncryptingEntityBenchmark {
    /**
     * Use the main method and not the constructor.
     */
    private EncryptingEntityBenchmark() {
    }

    /**
     * Method that runs the benchmark.
     *
     * @param tries number of times to execute
     * @param cipherDetails cipher implementation to benchmark
     * @throws IOException thrown when there is a problem streaming
     */
    private static void throughputTest(final int tries,
                                       final SupportedCipherDetails cipherDetails)
            throws IOException {
        final long oneMb = 1_048_576L;

        Duration[] durations = new Duration[tries];

        long totalMs = 0L;
        long totalMbs = 0L;
        long totalMbps = 0L;

        for (int i = 0; i < tries + 1; i++) {
            try (RandomInputStream in = new RandomInputStream(oneMb);
                 OutputStream noopOut = new NullOutputStream()) {

                MantaInputStreamEntity entity = new MantaInputStreamEntity(in, oneMb);
                SecretKey key = SecretKeyUtils.generate(cipherDetails);

                EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                        cipherDetails, entity);

                DigestedEntity digestedEntity = new DigestedEntity(encryptingEntity,
                        "MD5");

                long start = System.nanoTime();
                digestedEntity.writeTo(noopOut);
                long end = System.nanoTime();

                Duration duration = Duration.ofNanos(end - start);

                // We throw out the first try because the JVM is warming up
                if (i > 0) {
                    durations[i - 1] = duration;
                    long timeMs = duration.toMillis();
                    totalMs += timeMs;
                    long mbs = timeMs * 1000;
                    totalMbs += mbs;
                    long mbps = timeMs * 1000 * 8;
                    totalMbps += mbps;

                    System.out.printf("Total time=%dms, mbs=%d, mbps=%d\n",
                            timeMs, mbs, mbps);
                }
            }
        }

        System.out.printf("\nAverage time=%dms, mbs=%d, mbps=%d\n",
                totalMs / tries, totalMbs / tries, totalMbps / tries);
    }

    /**
     * Public entrance to the class.
     *
     * @param argv parameters
     * @throws Exception thrown when there is a problem streaming
     */
    public static void main(final String[] argv) throws Exception {
        final int tries;
        final SecureRandom random;

        if (argv.length < 1) {
            tries = 10;
        } else {
            tries = Integer.parseInt(argv[0].trim());
        }

        for (SupportedCipherDetails cipherDetails : SupportedCiphersLookupMap.INSTANCE.values()) {
            System.out.println("===================================================");
            System.out.printf(" %s Timings:\n", cipherDetails.getCipherId());
            System.out.println("===================================================");
            throughputTest(tries, cipherDetails);
        }
    }
}
