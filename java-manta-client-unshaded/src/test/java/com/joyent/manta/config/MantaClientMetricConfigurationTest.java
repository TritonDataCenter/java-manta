/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.config;

import com.codahale.metrics.MetricRegistry;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

@Test
public class MantaClientMetricConfigurationTest {
    public void rejectsInvalidInputs() throws Exception {
        Assert.assertThrows(NullPointerException.class, () ->
            new MantaClientMetricConfiguration(null, null));
        Assert.assertThrows(NullPointerException.class, () ->
            new MantaClientMetricConfiguration(null, null, null, null));

        Assert.assertThrows(NullPointerException.class, () ->
            new MantaClientMetricConfiguration(UUID.randomUUID(), new MetricRegistry(), MetricReporterMode.SLF4J, null));

        Assert.assertThrows(IllegalArgumentException.class, () ->
            new MantaClientMetricConfiguration(UUID.randomUUID(), new MetricRegistry(), MetricReporterMode.SLF4J, -1));

        Assert.assertThrows(IllegalArgumentException.class, () ->
            new MantaClientMetricConfiguration(UUID.randomUUID(), new MetricRegistry(), MetricReporterMode.SLF4J, 0));
    }
}
