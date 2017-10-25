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
import com.joyent.manta.config.TransactionalConfigContext;
import org.apache.commons.text.RandomStringGenerator;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

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

    private final TransactionalConfigContext transactionalConfigContext;

    public ConfigBenchmark() {
        standardConfig = new StandardConfigContext();
        standardConfig.setNoAuth(true);
        standardConfig.setMantaKeyId(GENERATOR.generate(10));

        authConfig = new AuthAwareConfigContext();
        authConfig.setNoAuth(true);
        authConfig.setMantaKeyId(GENERATOR.generate(10));

        transactionalConfigContext = new TransactionalConfigContext();
        transactionalConfigContext.setNoAuth(true);
        transactionalConfigContext.setMantaKeyId(GENERATOR.generate(10));
    }


    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String getKeyIdStandard() throws Exception {
        return standardConfig.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String getKeyIdAuth() throws Exception {
        return authConfig.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String getKeyIdTransactional() throws Exception {
        return transactionalConfigContext.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String updateAndGetKeyIdStandard() throws Exception {
        standardConfig.setMantaKeyId(GENERATOR.generate(10));
        return standardConfig.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String updateAndGetKeyIdAuth() throws Exception {
        authConfig.setMantaKeyId(GENERATOR.generate(10));
        return authConfig.getMantaKeyId();
    }

    @org.openjdk.jmh.annotations.Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public String updateAndGetKeyIdTransactional() throws Exception {
        transactionalConfigContext.setMantaKeyId(GENERATOR.generate(10));
        return transactionalConfigContext.getMantaKeyId();
    }
}
