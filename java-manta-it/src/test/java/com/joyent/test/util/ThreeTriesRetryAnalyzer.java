/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.test.util;

import org.testng.ITestResult;
import org.testng.Reporter;
import org.testng.util.RetryAnalyzerCount;

/**
 * Retries an integration test three times before it is considered a failure.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.2.4
 */
public class ThreeTriesRetryAnalyzer extends RetryAnalyzerCount {
    private static final int TRIES = 3;

    public ThreeTriesRetryAnalyzer() {
        setCount(TRIES);
    }

    @Override
    public boolean retryMethod(final ITestResult result) {
        // If successful, don't retry
        if (result.isSuccess()) {
            return false;
        }

        // If unsuccessful, but we still have retries
        if (getCount() > 0) {
            result.setStatus(ITestResult.SUCCESS_PERCENTAGE_FAILURE);

            final String msg = String.format(
                    "[%s]: Error in %s Retrying %d more times",
                    Thread.currentThread().getName(), result.getName(),
                    getCount());
            Reporter.log(msg);

            return true;
        }

        // If unsuccessful and we are out of retries
        return false;
    }
}
