/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.config.IntegrationTestConfigContext;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Test
public class MantaClientAuthenticationChangeIT {

    private IntegrationTestConfigContext config;

    private MantaClient mantaClient;

    private String testPathPrefix;

    @BeforeClass
    public void beforeClass() throws IOException {
        // Let TestNG configuration take precedence over environment variables
        config = new IntegrationTestConfigContext();

        mantaClient = new MantaClient(config);
        testPathPrefix = IntegrationTestConfigContext.generateBasePath(config, this.getClass().getSimpleName());
        mantaClient.putDirectory(testPathPrefix);
    }

    @AfterClass
    public void afterClass() throws IOException {
        Mockito.validateMockitoUsage();
        IntegrationTestConfigContext.cleanupTestDirectory(mantaClient, testPathPrefix);
    }

    public void canDisableAuthenticationAndStillHitPublicObjects() throws IOException {
        final String home = config.getMantaHomeDirectory();

        final List<String> homeListing = mantaClient.listObjects(home)
                .map(MantaObject::getPath)
                .collect(Collectors.toList());

        Assert.assertTrue(homeListing.contains(config.getMantaUser() + "/stor"));
        Assert.assertTrue(homeListing.contains(config.getMantaUser() + "/public"));
    }
}
