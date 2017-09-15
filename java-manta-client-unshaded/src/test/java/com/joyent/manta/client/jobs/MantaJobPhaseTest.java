/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import org.testng.annotations.Test;

/**
 * Tests the behavior of the validations and accessors of {@link MantaJobPhase}
 * instance.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobPhaseTest {

    @Test
    public void canCreateMapPhase() {
        new MantaJobPhase().setType("map");
    }

    @Test
    public void canCreateReducePhase() {
        new MantaJobPhase().setType("reduce");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cantCreateUnknownPhase() {
        new MantaJobPhase().setType("anything else");
    }

    @Test
    public void canSetCountForReduce() {
        new MantaJobPhase()
                .setType("reduce")
                .setCount(2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cantSetCountForMapPhase() {
        new MantaJobPhase()
                .setType("map")
                .setCount(2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cantSetBadCountValue() {
        new MantaJobPhase()
                .setCount(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cantSetBadMemoryValue() {
        new MantaJobPhase()
                .setMemory(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void cantSetBadDiskValue() {
        new MantaJobPhase()
                .setDisk(0);
    }
}
