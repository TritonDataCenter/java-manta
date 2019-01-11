/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaUnexpectedObjectTypeException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpRequestFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.joyent.manta.client.MantaDirectoryListingIterator.MAX_RESULTS;
import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test
public class MantaDirectoryListingIteratorTest {

    public static final int EOF = -1;

    @Mock
    private HttpHelper httpHelper;

    @Mock
    private CloseableHttpResponse response;

    @Mock
    private HttpEntity responseEntity;

    @BeforeMethod
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        MantaHttpRequestFactory requestFactory = new MantaHttpRequestFactory(UNIT_TEST_URL);

        when(httpHelper.getRequestFactory()).thenReturn(requestFactory);
        when(httpHelper.executeRequest(any(), any())).thenReturn(response);
        when(response.getEntity()).thenReturn(responseEntity);
    }

    @AfterMethod
    public void teardown() throws Exception {
        Mockito.validateMockitoUsage();
    }

    public void allowsSkippingFirstResult() throws Exception {
        String dirListing =
                "{\"name\":\".joyent\",\"type\":\"directory\",\"mtime\":\"2015-04-16T22:50:11.353Z\"}\n"
              + "{\"name\":\"foo\",\"etag\":\"2968452a-9f78-edbe-a5fa-fe167963f4cf\",\"type\":\"object\",\"contentType\":\"text/plain\",\"contentMD5\":\"1B2M2Y8AsgTpgAmY7PhCfg==\",\"mtime\":\"2017-04-16T23:20:12.393Z\"}";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE,
                MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);
        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});
        when(responseEntity.getContent()).thenReturn(IOUtils.toInputStream(
                dirListing, StandardCharsets.UTF_8));

        final MantaDirectoryListingIterator itr = new MantaDirectoryListingIterator(
                "/usr/stor/dir", httpHelper, MAX_RESULTS);

        itr.next();
        Map<String, Object> secondResult = itr.next();

        Assert.assertEquals(secondResult.get("name"), "foo");
        Assert.assertEquals(secondResult.get("etag"), "2968452a-9f78-edbe-a5fa-fe167963f4cf");
        Assert.assertEquals(secondResult.get("type"), "object");
        Assert.assertEquals(secondResult.get("contentType"), "text/plain");
        Assert.assertEquals(secondResult.get("contentMD5"), "1B2M2Y8AsgTpgAmY7PhCfg==");
        Assert.assertEquals(secondResult.get("mtime"), "2017-04-16T23:20:12.393Z");
        Assert.expectThrows(NoSuchElementException.class, itr::next);
    }

    public void throwsAppropriateExceptionWhenListingObject() throws Exception {
        final String dirPath = "/user/stor/directory";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE, ContentType.APPLICATION_OCTET_STREAM.toString());

        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getFirstHeader(CONTENT_TYPE)).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});

        // uncomment once it's okay to call next before hasNext
        Assert.expectThrows(MantaUnexpectedObjectTypeException.class, () ->
                new MantaDirectoryListingIterator(dirPath, httpHelper, MAX_RESULTS).next());

        Assert.expectThrows(MantaUnexpectedObjectTypeException.class, () ->
                new MantaDirectoryListingIterator(dirPath, httpHelper, MAX_RESULTS).hasNext());
    }

}
