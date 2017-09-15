/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests the behavior of the validations and accessors of {@link MantaJob}
 * instance.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobTest {
    @Test
    public void canGetMapPhases() {
        MantaJobPhase phase1 = new MantaJobPhase().setType("map").setExec("echo 1");
        MantaJobPhase phase2 = new MantaJobPhase().setType("map").setExec("echo 2");
        MantaJobPhase phase3 = new MantaJobPhase().setType("reduce").setExec("echo 3");
        List<MantaJobPhase> phases = new ArrayList<>();
        phases.add(phase1);
        phases.add(phase2);
        phases.add(phase3);

        MantaJob job = new MantaJob("test job", phases);

        List<MantaJobPhase> mapPhases = job.getMapPhases();

        Assert.assertEquals(mapPhases.get(0), phase1);
        Assert.assertEquals(mapPhases.get(1), phase2);
    }

    @Test
    public void canGetReducePhases() {
        MantaJobPhase phase1 = new MantaJobPhase().setType("map").setExec("echo 1");
        MantaJobPhase phase2 = new MantaJobPhase().setType("map").setExec("echo 2");
        MantaJobPhase phase3 = new MantaJobPhase().setType("reduce").setExec("echo 3");
        MantaJobPhase phase4 = new MantaJobPhase().setType("reduce").setExec("echo 4");

        List<MantaJobPhase> phases = new ArrayList<>();
        phases.add(phase1);
        phases.add(phase2);
        phases.add(phase3);
        phases.add(phase4);

        MantaJob job = new MantaJob("test job", phases);

        List<MantaJobPhase> reducePhases = job.getReducePhases();

        Assert.assertEquals(reducePhases.get(0), phase3);
        Assert.assertEquals(reducePhases.get(1), phase4);
    }
}
