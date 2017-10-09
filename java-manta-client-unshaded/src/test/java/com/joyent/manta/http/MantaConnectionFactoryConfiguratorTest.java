/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

@Test
public class MantaConnectionFactoryConfiguratorTest {

    @BeforeMethod
    public void setup() throws IOException {
        MockitoAnnotations.initMocks(this);
    }

    public void willValidateInputs() {
        Assert.assertThrows(() -> new MantaConnectionFactoryConfigurator(null));
    }

}
