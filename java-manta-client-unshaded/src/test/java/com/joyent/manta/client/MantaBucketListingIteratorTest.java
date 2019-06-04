/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaHttpRequestFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
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

import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.apache.http.HttpHeaders.CONTENT_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@Test
public class MantaBucketListingIteratorTest {

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
    public void teardown() {
        Mockito.validateMockitoUsage();
    }

    public void allowsSkippingFirstResult() throws Exception {
        String dirListing =
                "{\"name\":\".joyent\",\"type\":\"bucket\",\"mtime\":\"2015-04-16T22:50:11.353Z\"}\n"
                        + "{\"name\":\"foo\",\"type\":\"bucket\",\"mtime\":\"2017-04-16T23:20:12.393Z\"}";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE,
                MantaObjectResponse.BUCKET_RESPONSE_CONTENT_TYPE);
        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});
        when(responseEntity.getContent()).thenReturn(IOUtils.toInputStream(
                dirListing, StandardCharsets.UTF_8));

        final MantaBucketListingIterator itr = new MantaBucketListingIterator(
                "/usr/buckets", httpHelper);

        itr.next();
        Map<String, Object> secondResult = itr.next();

        Assert.assertEquals(secondResult.get("name"), "foo");
        Assert.assertEquals(secondResult.get("type"), "bucket");
        Assert.assertEquals(secondResult.get("mtime"), "2017-04-16T23:20:12.393Z");
        Assert.expectThrows(NoSuchElementException.class, itr::next);
    }
}
