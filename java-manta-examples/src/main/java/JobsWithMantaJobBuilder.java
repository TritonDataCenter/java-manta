/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.jobs.MantaJobBuilder;
import com.joyent.manta.client.jobs.MantaJobPhase;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;

import java.io.IOException;
import java.util.stream.Stream;

/**
 * Creating a job using the MantaJobBuilder allows for a more fluent style of
 * job creation. Using this approach allows for a more fluent configuration of
 * job initialization.
 */
public class JobsWithMantaJobBuilder {
    public static void main(String... args) throws IOException {
        ConfigContext config = new SystemSettingsConfigContext();
        MantaClient client = new MantaClient(config);

        // You can only get a builder from a MantaClient
        final MantaJobBuilder builder = client.jobBuilder();

        MantaJobBuilder.Run runningJob = builder.newJob("example")
                .addInputs("/user/stor/logs/input_1",
                        "/user/stor/logs/input_2",
                        "/user/stor/logs/input_3",
                        "/user/stor/logs/input_4")
                .addPhase(new MantaJobPhase()
                        .setType("map")
                        .setExec("grep foo"))
                .addPhase(new MantaJobPhase()
                        .setType("reduce")
                        .setExec("sort | uniq"))
                // This is an optional command that will validate that the inputs
                // specified are available
                .validateInputs()
                .run();

        // This will wait until the job is finished
        MantaJobBuilder.Done finishedJob = runningJob.waitUntilDone()
                // This will validate if the job finished without errors.
                // If there was an error an exception will be thrown
                .validateJobsSucceeded();

        // You will always need to close streams because we do everything online
        try (Stream<String> outputs = finishedJob.outputs()) {
            // Print each output
            outputs.forEach(System.out::println);
        }
    }
}
