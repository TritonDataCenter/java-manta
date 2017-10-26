/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ISuite;
import org.testng.ISuiteListener;
import org.testng.ITestContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test method listener for determining which tests will actually run.
 *
 * Enabled by: -Dit.dryrun=true
 */
public class TestListingInterceptor implements IMethodInterceptor, ISuiteListener {
    
    private static final Logger LOG = LoggerFactory.getLogger(TestListingInterceptor.class);

    private static final List<IMethodInstance> EMPTY_LIST = new ArrayList<>(0);

    private static final ConcurrentLinkedQueue<String> observedTests = new ConcurrentLinkedQueue<>();

    private static final boolean DRY_RUN_ENABLED;

    static {
        DRY_RUN_ENABLED = ObjectUtils.firstNonNull(
                BooleanUtils.toBoolean(System.getProperty("it.dryrun")),
                false);
    }

    public static int getObservedTestCount() {
        return observedTests.size();
    }

    @Override
    public List<IMethodInstance> intercept(final List<IMethodInstance> methods, final ITestContext context) {
        for (final IMethodInstance method : methods) {

            final String testName = method.getMethod().getQualifiedName();
            final Object instance = method.getInstance();

            String encryptionDetail = "";
            if (instance instanceof EncryptionAwareIntegrationTest) {
                final EncryptionAwareIntegrationTest encryptableInstance = ((EncryptionAwareIntegrationTest) instance);

                if (BooleanUtils.isNotFalse(encryptableInstance.isUsingEncryption())) {
                    encryptionDetail = '[' + encryptableInstance.getEncryptionCipher() + ']';
                }
            }

            observedTests.add(testName + ' ' + encryptionDetail);
        }

        if (DRY_RUN_ENABLED) {
            return EMPTY_LIST;
        }

        return methods;
    }

    @Override
    public void onStart(final ISuite suite) {
    }

    @Override
    public void onFinish(final ISuite suite) {
        if (!DRY_RUN_ENABLED) {
            return;
        }

        final String[] sortedTests = observedTests.toArray(new String[0]);
        Arrays.sort(sortedTests);

        System.out.println("DRY-RUN: Listing ["+ sortedTests.length +"] tests that would have run");

        for (final String testNameAndParams : sortedTests) {
            System.out.println(testNameAndParams);
        }
    }
}
