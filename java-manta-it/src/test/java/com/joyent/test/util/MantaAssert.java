/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaException;

import java.io.IOException;

/**
 * Assertions for verifying integration test behavior with Manta.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaAssert {
    /**
     * Functional wrapper that validates if a given closures throws a
     * {@link MantaClientHttpResponseException} and that it has the
     * correct status code.
     *
     * @param expectedStatusCode HTTP status code to verify
     * @param expectedErrorCode error code as returned from Manta
     * @param function block to verify
     * @param <R> return type
     * @return return value of underlying block
     */
    public static <R> R assertResponseFailureStatusCode(
            final int expectedStatusCode, final MantaErrorCode expectedErrorCode,
            final MantaFunction<R> function) {
        boolean thrown = false;
        int actualStatusCode = -1;
        MantaErrorCode actualErrorCode = MantaErrorCode.UNDEFINED;
        R result = null;

        try {
            result = function.apply();
        } catch (MantaClientHttpResponseException e) {
            actualStatusCode = e.getStatusCode();
            actualErrorCode = e.getServerCode();
            thrown = true;
        } catch (IOException e) {
            throw new MantaException(e);
        }

        if (!thrown) {
            String msg = String.format("Expected %s to be thrown, but it was not thrown",
                    MantaClientHttpResponseException.class);
            throw new AssertionError(msg);
        }

        if (actualStatusCode != expectedStatusCode) {
            String msg = String.format("Expected HTTP status code [%d] was: %d",
                    expectedStatusCode, actualStatusCode);
            throw new AssertionError(msg);
        }

        if (!actualErrorCode.equals(expectedErrorCode)) {
            String msg = String.format("Expected Manta error code [%s] was not received: %s",
                    expectedErrorCode, actualErrorCode);
            throw new AssertionError(msg);
        }

        return result;
    }
}
