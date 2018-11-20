/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.*;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.conn.ConnectTimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Integration tests for verifying the connection timeout behaviour for an invalid connection.
 * This Integration test is written for the sole purpose of verifying Issue #441.
 *
 * @author <a href="https://github.com/dekobon">Ashwin A Nair</a>
 */

@Test
public class TCPSocketConnectionTimeoutIT {
    private static final Logger LOG = LoggerFactory.getLogger(TCPSocketConnectionTimeoutIT.class);

    // Let TestNG configuration take precedence over environment variables
    private AuthAwareConfigContext authConfig = new AuthAwareConfigContext(
            new IntegrationTestConfigContext());

    private ChainedConfigContext config = new ChainedConfigContext(
                new StandardConfigContext()
                        .setTimeout(1000)
                        .setMantaUser(authConfig.getMantaUser())
                        .setMantaKeyId(authConfig.getMantaKeyId())
                        .setMantaKeyPath(authConfig.getMantaKeyPath())
                        /* This *should* be an invalid IP/port. 192.0.2.0/24
                           is intended to be reserved as TEST-NET-1 thus giving
                           us an invalid/unreachable MANTA_URL value. */
                        .setMantaURL("https://192.0.2.0:9324"),
                new DefaultsConfigContext()
        );

    public void verifyConnectionTimeoutSettingWorks() throws IOException {
        String testPathPrefix = IntegrationTestConfigContext.generateBasePath(
                config, this.getClass().getSimpleName());

        Instant start = null;
        Instant stop;

        try (MantaClient client = new MantaClient(config)) {
            start = Instant.now();
            client.head(testPathPrefix);

        } catch (ConnectTimeoutException e) {
            Assert.assertTrue(e.getMessage().endsWith("connect timed out"),
                    "ConnectTimeoutException didn't end with expected "
                            + "connection timeout message. Actual exception:\n"
                            + ExceptionUtils.getStackTrace(e));
        } finally {
            stop = Instant.now();
        }

        Duration totalTime = Duration.between(start, stop);

        if (totalTime.getSeconds() != 1) {
            String msg = String.format("Expected total connection time duration "
                    + "to be 1 second. Actually [%d] ms",
                    totalTime.toMillis());
            Assert.fail(msg);
        }
    }

    @AfterClass
    public void afterClass() throws Exception {
        authConfig.close();
    }
}