/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;

import java.io.IOException;

/**
 * Function allowing us to make useful testing lamdas.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@FunctionalInterface
public interface MantaFunction<R> {

    /**
     * A closure for wrapping sections of code for testing.
     * @return the function result
     */
    R apply() throws MantaClientHttpResponseException, MantaException, IOException;
}
