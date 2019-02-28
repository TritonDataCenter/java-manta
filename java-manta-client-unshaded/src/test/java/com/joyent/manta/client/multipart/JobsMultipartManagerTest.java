/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;
import com.joyent.manta.config.SystemSettingsConfigContext;
import com.joyent.manta.exception.MantaClientException;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

/* Tests are disabled because functionality is obsolete and overall test runtime
 * is very long and expensive. */
@Deprecated
@Test(enabled = false)
public class JobsMultipartManagerTest {
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void negativePartNumbersAreRejected() {
        JobsMultipartManager multipart = multipartInstance("user.name");
        multipart.validatePartNumber(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void zeroPartNumbersAreRejected() {
        JobsMultipartManager multipart = multipartInstance("user.name");
        multipart.validatePartNumber(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void partNumbersAboveMaxAreRejected() {
        JobsMultipartManager multipart = multipartInstance("user.name");
        multipart.validatePartNumber(multipart.getMaxParts() + 1);
    }

    public void canBuildMultiPartUploadPath() {
        final UUID id = new UUID(0L, 12L);
        JobsMultipartManager multipart = multipartInstance("user.name");
        String expected = String.format("/user.name/%s/%s/",
                JobsMultipartManager.MULTIPART_DIRECTORY, id);
        String actual = multipart.multipartUploadDir(id);
        assertEquals(expected, actual);
    }

    public void noErrorWhenAllPartsArePresentOrdered() throws IOException {
        final UUID id = new UUID(0L, 24L);
        final JobsMultipartUpload upload =  new JobsMultipartUpload(id, "/dev/null");

        List<MantaMultipartUploadPart> partsList = new ArrayList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            MantaMultipartUploadPart part = new MantaMultipartUploadPart(i, null, null);
            partsList.add(part);
        }

        JobsMultipartManager multiPart = spy(multipartInstance());
        doReturn(partsList.stream()).when(multiPart).listParts(upload);

        multiPart.validateThatThereAreSequentialPartNumbers(upload);
    }

    public void noErrorWhenAllPartsArePresentUnordered() throws IOException {
        final UUID id = new UUID(0L, 36L);
        final JobsMultipartUpload upload =  new JobsMultipartUpload(id, "/dev/null");

        List<MantaMultipartUploadPart> partsList = new ArrayList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            MantaMultipartUploadPart part = new MantaMultipartUploadPart(i, null, null);
            partsList.add(part);
        }

        Collections.shuffle(partsList);

        JobsMultipartManager multiPart = spy(multipartInstance());
        doReturn(partsList.stream()).when(multiPart).listParts(upload);

        multiPart.validateThatThereAreSequentialPartNumbers(upload);
    }

    public void errorWhenMissingPart() throws IOException {
        final UUID id = new UUID(0L, 48L);
        final JobsMultipartUpload upload =  new JobsMultipartUpload(id, "/dev/null");

        ArrayList<MantaMultipartUploadPart> partsList = new ArrayList<>();

        final int totalParts = 64;
        for (int i = 1; i <= totalParts; i++) {
            MantaMultipartUploadPart part = new MantaMultipartUploadPart(i, null, null);
            partsList.add(part);
        }

        partsList.remove(2);

        Collections.shuffle(partsList);

        JobsMultipartManager multiPart = spy(multipartInstance());
        doReturn(partsList.stream()).when(multiPart).listParts(upload);

        boolean thrown = false;

        try {
            multiPart.validateThatThereAreSequentialPartNumbers(upload);
        } catch (MantaClientException e) {
            if ((int)e.getFirstContextValue("missing_part") == 3) {
                thrown = true;
            }
        }

        assertTrue(thrown, "Exception wasn't thrown");
    }

    private JobsMultipartManager multipartInstance() {
        return multipartInstance("user.name");
    }

    private JobsMultipartManager multipartInstance(final String user) {

        final ConfigContext overwrite = new StandardConfigContext()
                .setMantaUser(user)
                .setNoAuth(true);
        final ConfigContext config = new SystemSettingsConfigContext(overwrite);

        // We set the manta client to a non-validating mode so that we can
        // unit test

        final MantaClient client = new MantaClient(config);
        return new JobsMultipartManager(client);
    }
}
