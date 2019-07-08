/*
 * Copyright (c) 2017-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.IntegrationTestConfigContext;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ISuite;
import org.testng.ISuiteListener;

import java.io.IOException;

public class MantaPathSuiteListener implements ISuiteListener {

    private static final Logger LOG = LoggerFactory.getLogger(MantaPathSuiteListener.class);

    private ConfigContext config;

    @Override
    public void onStart(ISuite suite) {
        config = new IntegrationTestConfigContext();
        LOG.info("Base manta path for suite {}: {}", suite.getName(),
                 IntegrationTestConfigContext.generateSuiteBasePath(config));
    }

    @Override
    public void onFinish(ISuite suite) {
        LOG.info("Expected test count: " + TestListingInterceptor.getObservedTestCount());

        MantaClient mantaClient = new MantaClient(config);
        String path = IntegrationTestConfigContext.generateSuiteBasePath(config);

        if (ObjectUtils.firstNonNull(
                BooleanUtils.toBoolean(System.getProperty("it.dryRun")),
                false)) {
            LOG.warn("Skipping suite cleanup since dry-run is enabled.");
            return;
        }

        try {
            if (mantaClient.isDirectoryEmpty(path)) {
                LOG.info("Removing base suite path: {}", path);
                mantaClient.delete(path);
            } else {
                LOG.warn("Base suite directory {} is not empty; leaving in place", path);
            }
        } catch (IOException e) {
            LOG.warn("Unable to check on base suite path: {}", path, e);
        }
    }

}
