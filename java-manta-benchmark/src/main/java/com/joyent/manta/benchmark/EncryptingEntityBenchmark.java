package com.joyent.manta.benchmark;

import com.joyent.manta.client.crypto.AesGcmCipherDetails;
import com.joyent.manta.client.crypto.EncryptingEntity;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.http.entity.DigestedEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.output.NullOutputStream;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
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
     * @param random source of entropy for the encryption algorithm
     * @throws IOException thrown when there is a problem streaming
     */
    private static void throughputTest(final int tries, final SecureRandom random)
            throws IOException {
        final long oneMb = 1_048_576L;
        final Charset charset = Charsets.US_ASCII;

        Duration[] durations = new Duration[tries];

        for (int i = 0; i < tries; i++) {
            try (RandomInputStream in = new RandomInputStream(oneMb);
                 OutputStream noopOut = new NullOutputStream()) {

                SupportedCipherDetails cipherDetails = AesGcmCipherDetails.INSTANCE;
                MantaInputStreamEntity entity = new MantaInputStreamEntity(in, oneMb);

                byte[] keyBytes = "FFFFFFFBD96783C6C91E2222".getBytes(charset);
                SecretKey key = SecretKeyUtils.loadKey(keyBytes, cipherDetails);

                EncryptingEntity encryptingEntity = new EncryptingEntity(key,
                        cipherDetails, oneMb, entity, random);

                DigestedEntity digestedEntity = new DigestedEntity(encryptingEntity,
                        "MD5");

                long start = System.nanoTime();
                digestedEntity.writeTo(noopOut);
                long end = System.nanoTime();

                Duration duration = Duration.ofNanos(end - start);
                durations[i] = duration;
                System.out.printf("Total time=%dms, mbs=%d, mbps=%d\n",
                        duration.toMillis(), duration.toMillis() * 1000,
                        (duration.toMillis() * 1000) * 8);
            }
        }
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

        if (argv.length < 2 && !argv[1].trim().equalsIgnoreCase("strong")) {
            random = new SecureRandom();
        } else {
            random = SecureRandom.getInstanceStrong();
        }

        throughputTest(tries, random);
    }
}
