/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

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
