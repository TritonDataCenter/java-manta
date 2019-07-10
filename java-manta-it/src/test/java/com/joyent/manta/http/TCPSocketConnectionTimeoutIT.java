/*
 * Copyright (c) 2018-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.helper.IntegrationTestHelper;
import com.joyent.manta.config.AuthAwareConfigContext;
import com.joyent.manta.config.ChainedConfigContext;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Integration tests for verifying the connection timeout behaviour for an invalid connection,
 * implies having an invalid value for MANTA_URL.
 * This Integration test is written for the sole purpose of verifying Issue #441.
 *
 * @author <a href="https://github.com/dekobon">Ashwin A Nair</a>
 */
@Test(groups = {"timeout"})
public class TCPSocketConnectionTimeoutIT {
    private MantaClient mantaClient;

    private String testPathPrefix;

    private ConfigContext config;

    public TCPSocketConnectionTimeoutIT(final @Optional String testType) throws IOException{
        // Let TestNG configuration take precedence over environment variables

        config = new IntegrationTestConfigContext();
        final String testName = this.getClass().getSimpleName();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestHelper.setupTestPath(config, mantaClient,
                testName, testType);
        IntegrationTestHelper.createTestBucketOrDirectory(mantaClient, testPathPrefix, testType);
    }

    @AfterClass
    public void cleanup() throws IOException {
        IntegrationTestHelper.cleanupTestBucketOrDirectory(mantaClient, testPathPrefix);
    }

    /**
     * Test that verifies that the connection timeout setting
     * {@link ConfigContext#getTimeout()} is applied and is actually working.
     *
     * Note: this test will not work correctly when run through a proxy server.
     */
    public void verifyConnectionTimeoutSettingWorks() throws IOException {
        final AuthAwareConfigContext authConfig = new AuthAwareConfigContext(
                new IntegrationTestConfigContext());

        final ConfigContext badConfig = new ChainedConfigContext(
                config,
                new StandardConfigContext()
                        .setRetries(0)
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

        Instant start = null;
        Instant stop;

        try (MantaClient client = new MantaClient(badConfig)){
            start = Instant.now();
            client.head(testPathPrefix);
        } catch (MantaIOException e) {
            final Throwable cause = ExceptionUtils.getThrowableList(e).get(1);

            Assert.assertEquals(cause.getClass().getSimpleName(),
                    "ConnectTimeoutException",
                    "Expected exception ConnectTimeoutException was not thrown. Actual Exception:\n"
                            + ExceptionUtils.getStackTrace(e));
        } finally {
            stop = Instant.now();
        }

        Duration totalTime = Duration.between(start, stop);

        if (totalTime.getSeconds() != 1) {
            final String msg = String.format("Expected total connection time duration "
                    + "to be 1 second. Actually [%d] ms",
                    totalTime.toMillis());
            Assert.fail(msg);
        }
    }
}
