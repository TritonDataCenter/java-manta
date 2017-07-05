/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.File;

@Test
public class ContentTypeLookupTest {
    private static final Header[] EXAMPLE_HEADERS = new Header[] {
        new BasicHeader("Last-Modified","Fri, 09 Dec 2016 22:09:44 GMT"),
        new BasicHeader("Result-Set-Size", "0"),
        new BasicHeader("Date", "Fri, 09 Dec 2016 22:09:44 GMT"),
        new BasicHeader("Server", "Manta"),
        new BasicHeader("x-request-id", "17d243b4-f9a7-4042-bc21-7322fb40fc1c"),
        new BasicHeader("x-response-time", "16"),
        new BasicHeader("x-server-name", "02d02889-cd80-4ac1-bc0c-4775b86661e4"),
        new BasicHeader("Connection", "keep-alive")};

    public void canFindDefault() {
        MantaHttpHeaders headers = new MantaHttpHeaders(EXAMPLE_HEADERS);
        ContentType troff = ContentType.create("application/x-troff");
        ContentType jsonStream = ContentType.create("application/x-json-stream");

        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(null, troff).getMimeType(),
                            troff.getMimeType());
        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers, troff).getMimeType(),
                            troff.getMimeType());
        headers.put("Content-Type", "application/x-json-stream; type=directory");
        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers, troff).getMimeType(),
                            jsonStream.getMimeType());
    }

    // These test will fail on some platforms where libgio will return
    // "text/plain" for empty files instead of indicating an unknown type.
    @Test(enabled = false)
    public void canfindByMultipleMethodsNull() throws Exception {
        MantaHttpHeaders headers = new MantaHttpHeaders(EXAMPLE_HEADERS);
        ContentType troff = ContentType.create("application/x-troff");
        ContentType jsonStream = ContentType.create("application/x-json-stream");

        File temp = File.createTempFile("upload", ".unknown");
        FileUtils.forceDeleteOnExit(temp);
        Assert.assertNull(ContentTypeLookup.findOrDefaultContentType(null, "/stor/unknown", temp, null));

        temp = File.createTempFile("upload", ".unknown");
        FileUtils.forceDeleteOnExit(temp);
        Assert.assertNull(ContentTypeLookup.findOrDefaultContentType(headers, "/stor/unknown", temp, null));
    }

    public void canfindByMultipleMethods() throws Exception {
        MantaHttpHeaders headers = new MantaHttpHeaders(EXAMPLE_HEADERS);
        ContentType troff = ContentType.create("application/x-troff");
        ContentType jsonStream = ContentType.create("application/x-json-stream");

        File temp = File.createTempFile("upload", ".jpeg");
        FileUtils.forceDeleteOnExit(temp);
        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers,
                                                                       "/stor/unknown",
                                                                       temp,
                                                                       troff).getMimeType(),
                            "image/jpeg");
        headers.put("Content-Type", "application/x-json-stream; type=directory");
        temp = File.createTempFile("upload", ".jpeg");
        FileUtils.forceDeleteOnExit(temp);
        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers,
                                                                       "/stor/unknown",
                                                                       temp,
                                                                       troff).getMimeType(),
                            jsonStream.getMimeType());
    }

    public void canFindByFilename() {
        MantaHttpHeaders headers = new MantaHttpHeaders(EXAMPLE_HEADERS);
        ContentType troff = ContentType.create("application/x-troff");
        ContentType jsonStream = ContentType.create("application/x-json-stream");

        Assert.assertNull(ContentTypeLookup.findOrDefaultContentType(null, "/tmp/unknown", null));
        Assert.assertNull(ContentTypeLookup.findOrDefaultContentType(headers, "/tmp/unknown", null));

        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers,
                                                                       "/tmp/unknown",
                                                                       troff).getMimeType(),
                            troff.getMimeType());

        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers,
                                                                       "/tmp/foo.jpeg",
                                                                       troff).getMimeType(),
                            "image/jpeg");
        headers.put("Content-Type", "application/x-json-stream; type=directory");
        Assert.assertEquals(ContentTypeLookup.findOrDefaultContentType(headers,
                                                                       "/tmp/foo.jpeg",
                                                                       troff).getMimeType(),
                            jsonStream.getMimeType());

    }
}
