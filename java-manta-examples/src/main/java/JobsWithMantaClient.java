/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.jobs.MantaJob;
import com.joyent.manta.client.jobs.MantaJobPhase;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Creating a job using the `MantaClient` API is done by making a number of calls
 * against the API and passing the job id to each API call. Here is an example that
 * processes 4 input files, greps them for 'foo' and returns the unique values.
 */
public class JobsWithMantaClient {
    public static void main(String... args) throws IOException, InterruptedException {
        ConfigContext config = new SystemSettingsConfigContext();
        MantaClient client = new MantaClient(config);

        List<String> inputs = new ArrayList<>();
        // You will need to change these to reflect the files that you want
        // to process
        inputs.add("/user/stor/logs/input_1");
        inputs.add("/user/stor/logs/input_2");
        inputs.add("/user/stor/logs/input_3");
        inputs.add("/user/stor/logs/input_4");

        List<MantaJobPhase> phases = new ArrayList<>();
        // This creates a map step that greps for 'foo' in all of the inputs
        MantaJobPhase map = new MantaJobPhase()
                .setType("map")
                .setExec("grep foo");
        // This returns unique values from all of the map outputs
        MantaJobPhase reduce = new MantaJobPhase()
                .setType("reduce")
                .setExec("sort | uniq");
        phases.add(map);
        phases.add(reduce);

        // This builds a job
        MantaJob job = new MantaJob("example", phases);
        UUID jobId = client.createJob(job);

        // This attaches the input data to the job
        client.addJobInputs(jobId, inputs.iterator());

        // This runs the job
        client.endJobInput(jobId);

        // This will get the status of the job
        MantaJob runningJob = client.getJob(jobId);

        // Wait until job finishes
        while (!client.getJob(jobId).getState().equals("done")) {
            Thread.sleep(3000L);
        }

        // Grab the results of the job as a string - in this case, there will
        // be only a single output
        // You will always need to close streams because we do everything online
        try (Stream<String> outputs = client.getJobOutputsAsStrings(jobId)) {
            // Print each output
            outputs.forEach(System.out::println);
        }
    }
}
