/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
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

import static com.joyent.manta.client.MantaBucketListingIterator.MAX_RESULTS;
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

    public void allowsSkippingFirstResultForBucket() throws Exception {
        String bucketListing =
                "{\"name\":\".joyent\",\"type\":\"bucket\",\"mtime\":\"2019-07-01T21:54:40.354Z\"}\n"
                        + "{\"name\":\"foo\",\"type\":\"bucket\",\"mtime\":\"2019-07-12T18:37:33.640Z\"}";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE,
                MantaObjectResponse.BUCKET_RESPONSE_CONTENT_TYPE);
        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});
        when(responseEntity.getContent()).thenReturn(IOUtils.toInputStream(
                bucketListing, StandardCharsets.UTF_8));

        final MantaBucketListingIterator itr = new MantaBucketListingIterator(
                "/usr/buckets", httpHelper, MAX_RESULTS);

        itr.next();
        Map<String, Object> secondResult = itr.next();

        Assert.assertEquals(secondResult.get("name"), "foo");
        Assert.assertEquals(secondResult.get("type"), "bucket");
        Assert.assertEquals(secondResult.get("mtime"), "2019-07-12T18:37:33.640Z");
        Assert.expectThrows(NoSuchElementException.class, itr::next);
    }

    public void allowsSkippingFirstResultForBucketObject() throws Exception {
        String bucketObjectsListing =
                "{\"name\":\".joyent\",\"type\":\"bucketobject\",\"etag\":\"2968452a-9f78-edbe-a5fa-fe167963f4cf\",\"size\":\"10\",\"contentType\":\"application/json; type=bucketobject\",\"contentMD5\":\"1B2M2Y8AsgTpgAmY7PhCfg==\",\"mtime\":\"2019-06-19T21:12:57.995Z\"}\n"
                        + "{\"name\":\"foo\",\"type\":\"bucketobject\",\"etag\":\"fc2133b3-716e-c888-96e7-fb7f0d356e58\",\"size\":\"18\",\"contentType\":\"application/json; type=bucketobject\",\"contentMD5\":\"1B2M2Y8AsgTpgAmY7PhCfg==\",\"mtime\":\"2019-06-19T21:14:16.448Z\"}";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE,
                MantaObjectResponse.BUCKETOBJECT_RESPONSE_CONTENT_TYPE);
        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});
        when(responseEntity.getContent()).thenReturn(IOUtils.toInputStream(
                bucketObjectsListing, StandardCharsets.UTF_8));

        final MantaBucketListingIterator itr = new MantaBucketListingIterator(
                "/usr/buckets/test-bucket/objects", httpHelper, MAX_RESULTS);

        itr.next();
        Map<String, Object> secondResult = itr.next();

        Assert.assertEquals(secondResult.get("name"), "foo");
        Assert.assertEquals(secondResult.get("type"), "bucketobject");
        Assert.assertEquals(secondResult.get("etag"), "fc2133b3-716e-c888-96e7-fb7f0d356e58");
        Assert.assertEquals(secondResult.get("size"), "18");
        Assert.assertEquals(secondResult.get("contentType"), "application/json; type=bucketobject");
        Assert.assertEquals(secondResult.get("contentMD5"), "1B2M2Y8AsgTpgAmY7PhCfg==");
        Assert.assertEquals(secondResult.get("mtime"), "2019-06-19T21:14:16.448Z");
        Assert.expectThrows(NoSuchElementException.class, itr::next);
    }

    public void throwsAppropriateExceptionWhenListingObject() {
        final String dirPath = "/user/stor/directory";
        final Header contentTypeHeader = new BasicHeader(CONTENT_TYPE, MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);

        when(responseEntity.getContentType()).thenReturn(contentTypeHeader);
        when(response.getFirstHeader(CONTENT_TYPE)).thenReturn(contentTypeHeader);
        when(response.getAllHeaders()).thenReturn( new Header[] {contentTypeHeader});

        Assert.expectThrows(MantaUnexpectedObjectTypeException.class, () ->
                new MantaBucketListingIterator(dirPath, httpHelper, MantaBucketListingIterator.MAX_RESULTS).next());

        Assert.expectThrows(MantaUnexpectedObjectTypeException.class, () ->
                new MantaBucketListingIterator(dirPath, httpHelper, MantaBucketListingIterator.MAX_RESULTS).hasNext());
    }
}
