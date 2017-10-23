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
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;
import org.testng.xml.XmlTest;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

// use with: -Dit.dryrun=true
// waiting on manta.no_auth to be a safe way to construct clients before we can actually use this
// (currently attempting to run use dryrun will fail when the test class cstrs attempt to build a MantaClient)
// https://stackoverflow.com/questions/26048398/list-tests-that-would-be-run-in-testng
public class TestListingInterceptor implements IMethodInterceptor {
    
    private static final Logger LOG = LoggerFactory.getLogger(TestListingInterceptor.class);

    private static final List<IMethodInstance> EMPTY_LIST = new ArrayList<>(0);

    private static final AtomicInteger observedTestCount = new AtomicInteger(0);

    public static int getObservedTestCount() {
        return observedTestCount.get();
    }

    @Override
    public List<IMethodInstance> intercept(final List<IMethodInstance> methods, final ITestContext context) {
        observedTestCount.addAndGet(methods.size());

        for (final IMethodInstance method : methods) {

            final String testName = method.getMethod().getQualifiedName();
            final Object instance = method.getInstance();

            String encryptionDetail = "";
            if (instance instanceof EncryptionAwareIT) {
                final EncryptionAwareIT encryptableInstance = ((EncryptionAwareIT) instance);

                if (BooleanUtils.isNotFalse(encryptableInstance.isUsingEncryption())) {
                    encryptionDetail = '[' + encryptableInstance.getEncryptionCipher() + ']';
                }
            }

            // Handling for data providers. If the method uses one, go invoke it
            // final Object[][] params = getParametersForMethod(method);

            System.err.println("TEST: " + testName + ' ' + encryptionDetail);
            // tests = params.collect { "${canonicalName}.$method.method.methodName(${it.join ", "})" }
        }

        if (ObjectUtils.firstNonNull(
                BooleanUtils.toBoolean(System.getProperty("it.dryrun")),
                false)) {
            return EMPTY_LIST;
        }

        return methods;
    }

    /*
     * When using factories, there's no way to distinguish instances the factory has created.
     * TestNG has a mechanism whereby you inherit from ITest, and implement a getTestName, which is typically
     * set during a @BeforeMethod call, and is used by reporters. This requires @BeforeMethod actually be called,
     * which we don't want to do from this interceptor. Instead, we ask the testclass to implement a property for
     * returning a serialized string called classParameters, which we will append to the canonicalName of the class
     */
    String getClassName(final IMethodInstance method) {
        final Class<?> realClass = method.getMethod().getRealClass();
        final String canonicalName = realClass.getCanonicalName();

        Method beforeMethod = null;
        Parameters parametersAnnotation = null;
        for (final Method classMethod : realClass.getMethods()) {
            if (classMethod.getAnnotation(BeforeClass.class) != null) {
                beforeMethod = classMethod;
                parametersAnnotation = classMethod.getAnnotation(Parameters.class);
                if (parametersAnnotation != null) {
                    break;
                }
            }
        }

        if (parametersAnnotation == null) {
            return canonicalName;
        }

        final String[] params = parametersAnnotation.value();

        if (params == null) {
            return canonicalName;
        }

        final XmlTest xmlTest = method.getMethod().getXmlTest();
        final Map<String, String> effectiveParameters = new HashMap<>();

        // for (final String parameterName : params) {
        //     effectiveParameters.put(parameterName, method.getMethod().)
        // }



        // if (realClass.metaClass.hasProperty(method.instance, "classParameters")) {
        //     canonicalName += "($method.instance.classParameters)"
        // }

        return canonicalName;
    }

    /*
     * The interceptor receives the list of methods before they have been exploded for
     */
    Object[][] getParametersForMethod(final IMethodInstance method) {
        final Test testAnnotation = method.getMethod().getConstructorOrMethod().getMethod().getAnnotation(Test.class);
        Class<?> dataProviderClass = testAnnotation.dataProviderClass();

        if (dataProviderClass == Object.class) {
            dataProviderClass = method.getMethod().getRealClass();
        }

        final String dataProviderName = testAnnotation.dataProvider();
        Method matchedDataProviderMethod = null;

        for (final Method dataProviderMethod : dataProviderClass.getMethods()) {
            final DataProvider dataProviderAnnotation = dataProviderMethod.getAnnotation(DataProvider.class);
            if (dataProviderAnnotation == null) {
                continue;
            }

            if (dataProviderAnnotation.name().equals(dataProviderName)) {
                matchedDataProviderMethod = dataProviderMethod;
                break;
            }
        }

        if (matchedDataProviderMethod == null) {
            return new Object[][]{};
        }

        try {
            return (Object[][]) matchedDataProviderMethod.invoke(method.getInstance());
        } catch (IllegalAccessException | InvocationTargetException e) {
            LOG.warn("Exception occurred while building dataprovider invocations", e);
            return new Object[][]{};
        }
    }



}
