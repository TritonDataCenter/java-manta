/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import org.testng.annotations.BeforeSuite;


/**
 * Class containing one time test suite setup methods.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class TestSuiteSetup {
    private Logger logger = LoggerFactory.getLogger(getClass());

    @BeforeSuite
    public void setupSuite() {
        logger.info("Setting up Java util logging to SLF4J bridge");

        // Remove existing handlers attached to j.u.l root logger
        SLF4JBridgeHandler.removeHandlersForRootLogger();  // (since SLF4J 1.6.5)

        // add SLF4JBridgeHandler to j.u.l's root logger, should be done once during
        // the initialization phase of your application
        SLF4JBridgeHandler.install();
    }
}
