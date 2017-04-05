/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.BeforeSuite;


/**
 * Class containing one time test suite setup methods.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class TestSuiteSetup {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @BeforeSuite
    public void setupSuite(final ITestContext context) {
        logger.info(context.getName());
    }
}
