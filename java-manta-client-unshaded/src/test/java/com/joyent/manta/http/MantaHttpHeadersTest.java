/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for verifying the behavior of HTTP header assignment.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@Test
public class MantaHttpHeadersTest {
    private static final Header[] DIR_LIST_HEADERS = new Header[] {
        new BasicHeader("Last-Modified","Fri, 09 Dec 2016 22:09:44 GMT"),
        new BasicHeader("Content-Type", "application/x-json-stream; type=directory"),
        new BasicHeader("Result-Set-Size", "0"),
        new BasicHeader("Date", "Fri, 09 Dec 2016 22:09:44 GMT"),
        new BasicHeader("Server", "Manta"),
        new BasicHeader("x-request-id", "17d243b4-f9a7-4042-bc21-7322fb40fc1c"),
        new BasicHeader("x-response-time", "16"),
        new BasicHeader("x-server-name", "02d02889-cd80-4ac1-bc0c-4775b86661e4"),
        new BasicHeader("Connection", "keep-alive")};

    public void canIngestApacheHeaders() {
        MantaHttpHeaders headers = new MantaHttpHeaders(DIR_LIST_HEADERS);

        for (Header h : DIR_LIST_HEADERS) {
            Object actual = headers.get(h.getName());
            Object expected = h.getValue();
            Assert.assertEquals(actual, expected);
        }

        int i = 0;
        Assert.assertEquals(headers.getLastModified(), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.getContentType(), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.getResultSetSize().toString(), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.getDate(), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.get(HttpHeaders.SERVER), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.getRequestId(), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.get("X-Response-Time"), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.get("X-Server-Name"), DIR_LIST_HEADERS[i++].getValue());
        Assert.assertEquals(headers.get(HttpHeaders.CONNECTION), DIR_LIST_HEADERS[i++].getValue());
    }

    public void canExportApacheHeaders() {
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setDurabilityLevel(3);
        headers.setContentType(ContentType.APPLICATION_JSON.withCharset(StandardCharsets.UTF_8).toString());
        headers.put("X-Multi-Header",
                Arrays.asList("value 1", "value 2"));

        Header[] apacheHeaders = headers.asApacheHttpHeaders();
        Header durability = findHeader(MantaHttpHeaders.HTTP_DURABILITY_LEVEL, apacheHeaders);
        Header contentType = findHeader(HttpHeaders.CONTENT_TYPE, apacheHeaders);
        Header multiHeader = findHeader("x-multi-header", apacheHeaders);

        Assert.assertEquals(headers.getDurabilityLevel().toString(), durability.getValue());
        Assert.assertEquals(headers.getContentType(), contentType.getValue());

        @SuppressWarnings("unchecked")
        Collection<String> multiHeaderValues = (Collection<String>)headers.get("x-multi-header");
        Assert.assertEquals(StringUtils.join(multiHeaderValues, ", "), multiHeader.getValue());
    }

    public void canSetRoleTags() {
        final MantaHttpHeaders headers = new MantaHttpHeaders();
        final Set<String> roles = new HashSet<>();
        roles.add("admin");
        roles.add("read-only");
        roles.add("reporting");

        headers.setRoles(roles);

        Set<String> actual = headers.getRoles();

        if (!CollectionUtils.isEqualCollection(actual, roles)) {
            Assert.fail("Input and output roles, should be equal");
        }
    }

    private static Header findHeader(final String name, final Header[] headers) {
        for (Header h : headers) {
            if (h.getName().toLowerCase().equals(name.toLowerCase())) {
                return h;
            }
        }

        return null;
    }
}
