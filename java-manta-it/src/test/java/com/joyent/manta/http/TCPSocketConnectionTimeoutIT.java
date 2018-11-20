/*
 * Copyright (c) 2015-2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;



/**
 * Integration tests for verifying the connection timeout behaviour for an invalid connection.
 * Untangling different socket-timeouts.
 * This Integration test is written for the sole purpose of verifying Open Issue #441.
 *
 * @author <a href="https://github.com/dekobon">Ashwin A Nair</a>
 */

@Test
public class TCPSocketConnectionTimeoutIT {

    private static final Logger LOG = LoggerFactory.getLogger(TCPSocketConnectionTimeoutIT.class);
    // Let TestNG configuration take precedence over environment variables
    private AuthAwareConfigContext authConfig = new AuthAwareConfigContext(new IntegrationTestConfigContext());

    private ChainedConfigContext config = new ChainedConfigContext(
                new StandardConfigContext()
                        .setTimeout(5000)
                        .setTcpSocketTimeout(5000)
                        .setMantaUser(authConfig.getMantaUser())
                        .setMantaKeyId(authConfig.getMantaKeyId())
                        .setMantaKeyPath(authConfig.getMantaKeyPath())
                        .setMantaURL("https://192.168.7.99"), // An invalid/unreachable MANTA_URL value
                new DefaultsConfigContext()

        );

    public void testConnectionTimeout() throws IOException {

        System.setProperty("manta.dumpConfig", "true");

        MantaClient client = new MantaClient(config);
        String testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());

        Instant start= Instant.now();
        Instant stop;

        try {
            start = Instant.now();
            client.head(testPathPrefix);

        } catch (IOException e) {
            LOG.error("TCP-Socket Connection Timeout Exception", e);

        } finally {
            stop = Instant.now();
        }

        Duration totalTime = Duration.between(start,stop);
        System.out.printf("Total Time taken for connection Time-Out: %d s",totalTime.getSeconds());


    }

    @AfterClass
    public void afterClass() throws Exception {
        authConfig.close();
    }
}