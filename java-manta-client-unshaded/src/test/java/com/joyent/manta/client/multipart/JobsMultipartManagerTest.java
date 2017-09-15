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
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

@Test
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

        List<MantaMultipartUploadPart> partsList = new LinkedList<>();

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

        List<MantaMultipartUploadPart> partsList = new LinkedList<>();

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
        final String privateKey = "-----BEGIN RSA PRIVATE KEY-----\n" +
                "MIIEpQIBAAKCAQEA1lPONrT34W2VPlltA76E2JUX/8+Et7PiMiRNWAyrATLG7aRA\n" +
                "8iZ5A8o/aQMyexp+xgXoJIh18LmJ1iV8zqnr4TPXD2iPO92fyHWPu6P+qn0uw2Hu\n" +
                "ZZ0IvHHYED+fqxm7jz2ZjnfZl5Bz73ctjRF+77rPgOhhfv4KAc1d9CDsC+lHTqbp\n" +
                "ngufCYI4UWrnYoQ2JVXvEL9D5dMlHg0078qfh2cPg5xMOiOYobZeWqflV1Ue5I1Y\n" +
                "owNqiFzIDmBK0TKhnv+qQVNfMnNLJBYlYyGd0DUOJs8os5yivtuQXOhLZ0zLiTqK\n" +
                "JVjNJLzlcciqUf97Btm2enEHJ/khMFhrmoTQFQIDAQABAoIBAQCdc//grN4WHD0y\n" +
                "CtxNjd9mhVGWOsvTcTFRiN3RO609OiJuXubffmgU4rXm3dRuH67Wp2w9uop6iLO8\n" +
                "QNoJsUd6sGzkAvqHDm/eAo/PV9E1SrXaD83llJHgbvo+JZ+VQVhLCQQQZ/fQouyp\n" +
                "FbK/GgVY9LKQjydg9hw/6rGFMdJ3hFZVFqYFUhNpQKpczi6/lI/UIGcBhF3+8s/0\n" +
                "KMrz2PcCQFixlUFtBYXQHarOctxJDX7indchX08buwPqSv4YBBDLHUZkkMWomI/P\n" +
                "NjRDRyqnxvI03lHVfdbDzoPMxklJlHF68fkmp8NFLegnCBM8K0ae65Vk61b3oF9X\n" +
                "3eD6JtAZAoGBAPo/oBaJlA0GbQoJmULj6YqcQ2JKbUJtu7LP//8Gss47po4uqh6n\n" +
                "9vneKEpYYxuH5MXNsqtinmSQQMkE4UXoJSxJvnXNVAMQa3kUd0UgZSHjqWWgauDj\n" +
                "BjLQRpy9evef7VzTYx0xqEfAprsXxAoy0KXYN8gwgMC6MQgfZuFBgtxLAoGBANtA\n" +
                "1SVN/4wqrz4C8rpx7oZarHcMmGLiFF5OpKXlq1JY+U8IJ+WxMId3TI4h/h6OQGth\n" +
                "NJzQqFCS9H3a5EmqoNXHsLVXiKtG40+OzphSf9Y/NU7FtKanFWjfZl1ihhran1Fc\n" +
                "42jzN34EMM7Wm8p6HUK5qiDSCF+Ck0Lupud+WIkfAoGAXREOg3M0+UcbhDEfq23B\n" +
                "bAhDUymkyqCuvoh2hyzBkMtEXPpj0DTdN/3z8/o9GX8HiLzAJtbtWy7+uQO0l+AG\n" +
                "+xqN15e+F8mifowq8y1iDyFw3Ve0h+BGbN1idWZOdgsnJm+DG9dc4xp1p3zmLnjJ\n" +
                "efQYgr3vFD3qgD/Vbg6EEVMCgYEAnNfaIh+T6Y83YWL2hI2wFgiTS26FLGeSLoyP\n" +
                "l+WeEwB3CCRLdjK1BpM+/oYupWkZiDc3Td6uKUWXBNkrac9X0tZRAMinie7h+S2t\n" +
                "eKW7sWXyGnGv82+fDzCQp8ktKdSvF6MdQxyJ2+nfiHdZZxTIDc2HeIcHWlusQLs8\n" +
                "RmnJp/0CgYEA8AUV7K2KNRcwfuB1UjqhvlaqgiGixrItacGgnMQJ2cRSRSq2fZTm\n" +
                "eXxT9ugZ/9J9D4JTYZgdABnKvDjgbJMH9w8Nxr+kn/XZKNDzc1z0iJYwvyBOc1+e\n" +
                "prHvy4y+bCc0kLjCNQW4+/pVTWe1w8Mp63Vhdn+fO+wUGT3DTJGIXkU=\n" +
                "-----END RSA PRIVATE KEY-----";
        final String fingerprint = "ac:95:92:ff:88:f7:3d:cd:ba:23:7b:54:44:21:60:02";

        final ConfigContext overwrite = new StandardConfigContext()
                .setMantaUser(user)
                .setMantaKeyId(fingerprint)
                .setPrivateKeyContent(privateKey);
        final ConfigContext config = new SystemSettingsConfigContext(overwrite);

        // We set the manta client to a non-validating mode so that we can
        // unit test
        try {
            System.setProperty("manta.dontValidateConfig", "true");
            final MantaClient client = new MantaClient(config);
            return new JobsMultipartManager(client);
        } finally {
            System.setProperty("manta.dontValidateConfig", "false");
        }
    }
}
