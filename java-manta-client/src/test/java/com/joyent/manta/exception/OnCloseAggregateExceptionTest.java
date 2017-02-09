/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class OnCloseAggregateExceptionTest {
    public void canAggregateExceptions() {
        String msg = "Exception message";
        OnCloseAggregateException exception = new OnCloseAggregateException(msg);

        for (int i = 1; i < 11; i++) {
            Exception inner = new RuntimeException("Exception " + i);
            exception.aggregateException(inner);
        }

        int entries = exception.getContextEntries().size();
        Assert.assertEquals(entries, 10);
    }
}
