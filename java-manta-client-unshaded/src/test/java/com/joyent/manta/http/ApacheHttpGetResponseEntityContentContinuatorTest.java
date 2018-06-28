/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadIncompatibleRequestException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicRequestLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Map;

import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.INFINITE_CONTINUATIONS;
import static com.joyent.manta.http.HttpRange.fromContentLength;
import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test
public class ApacheHttpGetResponseEntityContentContinuatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpGetResponseEntityContentContinuatorTest.class);

    public void httpGetIsCloneable() throws ResumableDownloadIncompatibleRequestException {

        final String initialRange = new HttpRange.Request(0, 2).render();

        final HttpGet get = new HttpGet(UNIT_TEST_URL);
        get.setHeaders(new Header[]{
                new BasicHeader(RANGE, initialRange),
                new BasicHeader(IF_MATCH, "abc")});

        final HttpGet cloned = ApacheHttpGetResponseEntityContentContinuator.cloneRequest(get);
        // we have to call getValue because most of the header classes dont actually equals themselves
        assertEquals(cloned.getFirstHeader(RANGE).getValue(), get.getFirstHeader(RANGE).getValue());

        cloned.setHeader(RANGE, new HttpRange.Request(1, 2).render());
        assertNotEquals(cloned.getFirstHeader(RANGE).getValue(), get.getFirstHeader(RANGE).getValue());
    }

    public void ctorRejectsInvalidInput() throws Exception {
        final MantaApacheHttpClientContext connCtx = mock(MantaApacheHttpClientContext.class);
        when(connCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        final HttpGet request = new HttpGet();
        final HttpResponse response =
                prepareResponseWithHeaders(
                        unmodifiableMap(
                                ETAG, singleValueHeaderList(ETAG, "a"),
                                CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, "1")));

        // basic null checks
        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(null, null, null, INFINITE_CONTINUATIONS));


        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, null, response, INFINITE_CONTINUATIONS));

        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, null, INFINITE_CONTINUATIONS));

        // this connectionContext returns null for the getHttpClient call
        final MantaApacheHttpClientContext badConnCtx = mock(MantaApacheHttpClientContext.class);
        assertThrows(NullPointerException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(badConnCtx, request, response, INFINITE_CONTINUATIONS));

        // retry count validation
        assertThrows(IllegalArgumentException.class, () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, response, 0));
        assertThrows(IllegalArgumentException.class, () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, response, -3));

        // this connectionContext somehow doesn't support retry cancellation
        // e.g. a custom MantaConnectionFactory or MantaConnectionContext was supplied
        final MantaApacheHttpClientContext retryNotCancellableConnCtx = mock(MantaApacheHttpClientContext.class);
        when(retryNotCancellableConnCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        when(retryNotCancellableConnCtx.isRetryEnabled()).thenReturn(true);
        when(retryNotCancellableConnCtx.isRetryCancellable()).thenReturn(false);

        assertThrows(ResumableDownloadException.class,
                () -> new ApacheHttpGetResponseEntityContentContinuator(retryNotCancellableConnCtx, request, response, INFINITE_CONTINUATIONS));
    }

    public void validatesNoCollidingHeadersForInitialRequest() throws Exception {
        validatesCompatibleHeadersForInitialRequest(emptyMap());
    }

    public void validatesCompatibleIfMatchHeaderForInitialRequest() throws Exception {
        final String ifMatchHeaderValue = new HttpRange.Request(0, 10).render();

        // users are allowed to omit or set their own if-match header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, singleValueHeaderList(IF_MATCH, ifMatchHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{null}));
        });

        // the following is most likely user error, though we should ideally disallow it
        // (if-match: "" doesn't make sense for GET, only for PUT, and this is all about resumable *downloads*)
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "")));
        });

        // we make no effort to merge headers
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(
                            IF_MATCH, new Header[]{new BasicHeader(IF_MATCH, ifMatchHeaderValue), new BasicHeader(
                                    IF_MATCH, ifMatchHeaderValue)}));
        });
    }

    public void validatesCompatibleRangeHeaderForInitialRequest() throws Exception {
        final String rangeHeaderValue = new HttpRange.Request(0, 10).render();

        // users are allowed to omit or set their own initial range header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, singleValueHeaderList(RANGE, rangeHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{null}));
        });

        // the following are most likely user error
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "")));
        });

        final ResumableDownloadIncompatibleRequestException malformed = expectThrows(
                ResumableDownloadIncompatibleRequestException.class, () -> {
                    validatesCompatibleHeadersForInitialRequest(
                            unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "duck")));
                });

        assertTrue(malformed.getCause() != null && malformed.getCause() instanceof HttpException);
        assertTrue(malformed.getMessage().contains("duck"));

        // we explicitly do not handle resuming multiple ranges (though we could in the future)
        // (supplying the same range header twice is also an error, but we're not concerned with that)
        assertThrows(ResumableDownloadIncompatibleRequestException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(
                            RANGE, new Header[]{new BasicHeader(RANGE, rangeHeaderValue), new BasicHeader(
                                    RANGE,
                                    rangeHeaderValue)}));
        });
    }

    public void complainsAboutBothHeadersWhenRangeAndIfMatchAreMalformed() throws Exception {
        final ResumableDownloadIncompatibleRequestException e = expectThrows(
                ResumableDownloadIncompatibleRequestException.class, () -> {
                    validatesCompatibleHeadersForInitialRequest(
                            unmodifiableMap(
                                    RANGE, singleValueHeaderList(RANGE, ""),
                                    IF_MATCH, singleValueHeaderList(IF_MATCH, "")));
                });

        assertTrue(e.getMessage().contains(RANGE));
        assertTrue(e.getMessage().contains(IF_MATCH));
    }

    public void createMarkerValidatesHints() throws Exception {
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        // any returned range is fine since none was specified
        final HttpRange.Response range = new HttpRange.Response(0, 1, 2L);

        // invalid ETag
        final String badEtag = etag.substring(1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(null, null)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(null, range)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                ResumableDownloadMarker.validateInitialExchange(
                        ImmutablePair.of(etag, null), ImmutablePair.of(badEtag, range)));

        assertNotNull(
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                        ImmutablePair.of(etag, range)));
    }

    /**
     * Tests validation of responses for downloads with an initial response code 200.
     */
    public void validateResponseExpectsNonNullHintsAndResponseFingerprint() throws Exception {
        final HttpGet req = new HttpGet();

        final String etag = "abc";
        final long contentLength = 2;
        final HttpRange.Response contentRange = fromContentLength(HttpRange.Response.class, contentLength);
        final HttpResponse res = prepareResponseWithHeaders(
                unmodifiableMap(
                        ETAG, singleValueHeaderList(ETAG, etag),
                        CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, Long.toString(contentLength)),
                        CONTENT_RANGE, singleValueHeaderList(CONTENT_RANGE, contentRange.render())));

        new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.of(etag, contentRange));

        // the following assertion just tests for programmer error
        assertThrows(NullPointerException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(null));

        // the following assertions test a response with insufficient headers
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.nullPair()));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.of(etag, null)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.of(null, contentRange)));

        // the following assertions test a response with incorrect headers
        final String badEtag = etag.substring(1);
        final HttpRange.Response badContentRange = new HttpRange.Response(0, contentLength, contentLength + 1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.of(etag, badContentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(ImmutablePair.of(badEtag, contentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class, () ->
                new ApacheHttpGetResponseEntityContentContinuator(req, res).validateResponseWithMarker(
                        ImmutablePair.of(badEtag, badContentRange)));
    }

    private static final int[] BUFFER_SIZES = new int[]{1, 2, 3, 4, 5, 7, 8, 9, 127, 128, 129, 1023, 1024, 1025, 2048};

    private static void validatesCompatibleHeadersForInitialRequest(final Map<String, Header[]> headers)
            throws Exception {
        final HttpGet req = prepareRequestWithHeaders(headers);
        verifyNoMoreInteractions(req);

        ApacheHttpGetResponseEntityContentContinuator.ensureRequestHeadersAreCompatible(req);
    }

    private static Header[] singleValueHeaderList(final String name, final String value) {
        return new Header[]{new BasicHeader(name, value)};
    }

    private static HttpGet prepareRequestWithHeaders(final Map<String, Header[]> headers) {
        final HttpGet req = prepareMessageWithHeaders(HttpGet.class, headers);

        when(req.getRequestLine()).thenReturn(new BasicRequestLine(HttpGet.METHOD_NAME, "", HttpVersion.HTTP_1_1));

        return req;
    }

    private static HttpResponse prepareResponseWithHeaders(final Map<String, Header[]> headers) {
        return prepareMessageWithHeaders(HttpResponse.class, headers);
    }

    private static <T extends HttpMessage> T prepareMessageWithHeaders(final Class<T> klass,
                                                                       final Map<String, Header[]> headers) {
        final T msg = mock(klass);

        // return an empty list unless a list of headers was provided
        when(msg.getHeaders(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            return headers.getOrDefault(headerName, new Header[0]);
        });

        when(msg.getFirstHeader(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            if (!headers.containsKey(headerName)) {
                return null;
            }

            final Header[] matched = headers.get(headerName);
            if (matched.length == 0) {
                return null;
            }

            return matched[0];
        });

        for (final Map.Entry<String, Header[]> headerNameAndList : headers.entrySet()) {
            final String headerName = headerNameAndList.getKey();
            when(msg.getHeaders(headerName)).thenReturn(headerNameAndList.getValue());
        }

        return msg;
    }
}
