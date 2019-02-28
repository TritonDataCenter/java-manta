/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.benchmark;

import com.joyent.manta.client.crypto.EncryptingEntity;
import com.joyent.manta.client.crypto.SecretKeyUtils;
import com.joyent.manta.client.crypto.SupportedCipherDetails;
import com.joyent.manta.client.crypto.SupportedCiphersLookupMap;
import com.joyent.manta.http.entity.DigestedEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import com.twmacinta.util.FastMD5Digest;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.util.NullOutputStream;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;


/**
 * Benchmark for determining the throughput of an encryption algorithm.
 */
@State(Scope.Thread)
@SuppressWarnings({"checkstyle:javadocmethod", "checkstyle:javadoctype", "checkstyle:javadocvariable",
            "checkstyle:visibilitymodifier"})
public class EncryptingEntityBenchmark {

    @Param({"AES128/GCM/NoPadding", "AES192/GCM/NoPadding", "AES256/GCM/NoPadding",
                "AES128/CTR/NoPadding", "AES192/CTR/NoPadding",
                "AES256/CTR/NoPadding", "AES128/CBC/PKCS5Padding",
                "AES192/CBC/PKCS5Padding", "AES256/CBC/PKCS5Padding",
                "AES128/CTR/NoPadding"})
    private String encryptionAlgorithm;

    // NOTE: EncryptingEntity requires modification for this to be
    // configurable for testing
    @Param({"128"})
    @SuppressWarnings({"UnusedVariable", "unused"})
    private int blockSize;

    private SupportedCipherDetails cipherDetails;
    private SecretKey secretKey;

    @Setup
    public void setup() {
        cipherDetails = SupportedCiphersLookupMap.INSTANCE.get(encryptionAlgorithm);
        secretKey = SecretKeyUtils.generate(cipherDetails);
        System.out.println("\n#Provider: " + cipherDetails.getCipher().getProvider().getName());
    }


    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public OutputStream encryptOneMiB() throws IOException {
        final long oneMib = 1024 * 1024;
        try (RandomInputStream in = new RandomInputStream(oneMib);
             OutputStream noopOut = new NullOutputStream()) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, oneMib);
            EncryptingEntity encryptingEntity = new EncryptingEntity(secretKey,
                                                                     cipherDetails, entity);
            encryptingEntity.writeTo(noopOut);
            return noopOut;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public OutputStream encryptAndDigestOneMiB() throws IOException {
        final long oneMib = 1024 * 1024;
        try (RandomInputStream in = new RandomInputStream(oneMib);
             OutputStream noopOut = new NullOutputStream()) {
            MantaInputStreamEntity entity = new MantaInputStreamEntity(in, oneMib);
            EncryptingEntity encryptingEntity = new EncryptingEntity(secretKey,
                                                                     cipherDetails, entity);
            DigestedEntity digestedEntity = new DigestedEntity(encryptingEntity,
                                                               new FastMD5Digest());
            digestedEntity.writeTo(noopOut);
            return noopOut;
        }
    }
}
