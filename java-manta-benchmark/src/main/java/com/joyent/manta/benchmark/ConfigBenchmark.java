/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.benchmark;

import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import org.apache.commons.text.RandomStringGenerator;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Thread)
@SuppressWarnings({"checkstyle:javadocmethod",
                          "checkstyle:javadoctype",
                          "checkstyle:javadocvariable",
                          "checkstyle:visibilitymodifier",
                          "checkstyle:magicnumber"})
public class ConfigBenchmark {

    private static final RandomStringGenerator GENERATOR =
            new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    private final StandardConfigContext standardConfig;

    private final AuthAwareConfigContext authConfig;

    private final AtomicLong num = new AtomicLong(0);

    public ConfigBenchmark() {
        standardConfig = new StandardConfigContext();
        standardConfig.setNoAuth(true);

        authConfig = new AuthAwareConfigContext(new StandardConfigContext());
        authConfig.setNoAuth(true);
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String updateAndGetKeyId() throws IOException {
        standardConfig.setMantaKeyId(GENERATOR.generate(10));

        return standardConfig.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String updateAuthAndGetKeyId() throws IOException {
        authConfig.setMantaKeyId(GENERATOR.generate(10));

        return authConfig.getMantaKeyId();
    }
}
