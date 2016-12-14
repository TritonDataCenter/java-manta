/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeSuite;


/**
 * Class containing one time test suite setup methods.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class TestSuiteSetup {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @BeforeSuite
    public void setupSuite() {
        logger.info("Setting up test suite");
    }
}
