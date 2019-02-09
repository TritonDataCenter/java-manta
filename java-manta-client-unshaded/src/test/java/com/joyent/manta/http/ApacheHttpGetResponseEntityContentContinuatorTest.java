/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.joyent.manta.exception.HttpDownloadContinuationException;
import com.joyent.manta.exception.HttpDownloadContinuationUnexpectedResponseException;
import com.joyent.manta.http.HttpRange.BoundedRequest;
import com.joyent.manta.http.entity.NoContentEntity;
import com.joyent.manta.util.InputStreamContinuator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.ProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.INFINITE_CONTINUATIONS;
import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.METRIC_NAME_CONTINUATIONS_PER_REQUEST;
import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.METRIC_NAME_RECOVERED_EXCEPTION_PREFIX;
import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadRequestFingerprint;
import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadResponseFingerprint;
import static com.joyent.manta.http.ApacheHttpTestUtils.prepareResponseWithHeaders;
import static com.joyent.manta.http.ApacheHttpTestUtils.singleValueHeaderList;
import static com.joyent.manta.http.HttpRange.fromContentLength;
import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.apache.http.HttpHeaders.CONTENT_LENGTH;
import static org.apache.http.HttpHeaders.CONTENT_RANGE;
import static org.apache.http.HttpHeaders.ETAG;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class ApacheHttpGetResponseEntityContentContinuatorTest {

    private static final Logger LOG = LoggerFactory.getLogger(ApacheHttpGetResponseEntityContentContinuatorTest.class);

    public static final String STUB_ETAG = "abc";

    private static final byte[] STUB_CONTENT = new byte[]{'f', 'o', 'o',};

    private static final long STUB_CONTENT_LENGTH = STUB_CONTENT.length;

    private static final HttpRange.Response STUB_CONTENT_RANGE = new HttpRange.Response(0, 2, 3);

    private static final ByteArrayEntity STUB_RESPONSE_ENTITY = new ByteArrayEntity(STUB_CONTENT);

    private static final HttpDownloadContinuationMarker STUB_MARKER = new HttpDownloadContinuationMarker(STUB_ETAG,
                                                                                                         STUB_CONTENT_RANGE);

    private static final ApacheHttpGetResponseEntityContentContinuator STUB_CONTINUATOR;

    static {
        final HttpClient client = mock(HttpClient.class);
        final HttpGet req = new HttpGet();
        req.setHeader(RANGE, fromContentLength(BoundedRequest.class, STUB_CONTENT_LENGTH).render());

        final HttpRange.Response STUB_CONTENT_RANGE = fromContentLength(HttpRange.Response.class, STUB_CONTENT_LENGTH);
        final HttpResponse STUB_BOUNDED_RESPONSE = prepareResponseWithHeaders(
                unmodifiableMap(
                        ETAG, singleValueHeaderList(ETAG, STUB_ETAG),
                        CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, Long.toString(STUB_CONTENT_LENGTH)),
                        CONTENT_RANGE, singleValueHeaderList(CONTENT_RANGE, STUB_CONTENT_RANGE.render())));

        ApacheHttpGetResponseEntityContentContinuator continuator;
        HttpDownloadContinuationMarker marker;
        try {
            marker = HttpDownloadContinuationMarker.validateInitialExchange(
                    extractDownloadRequestFingerprint(req),
                    SC_PARTIAL_CONTENT,
                    // don't allow the Content-Range to be inferred by passing false
                    extractDownloadResponseFingerprint(STUB_BOUNDED_RESPONSE, false));
            continuator = new ApacheHttpGetResponseEntityContentContinuator(client, req, marker, INFINITE_CONTINUATIONS, null);
        } catch (final ProtocolException pe) {
            LOG.error("There was an unexpected error constructing the stub continuator, expect NPEs");
            continuator = null;
        }

        STUB_CONTINUATOR = continuator;
    }

    public void canCloneHttpGet() {
        final String initialRange = new BoundedRequest(0, 2).render();

        final HttpGet get = new HttpGet(UNIT_TEST_URL);
        get.setHeaders(new Header[]{
                new BasicHeader(RANGE, initialRange),
                new BasicHeader(IF_MATCH, "abc")});

        final HttpGet cloned = ApacheHttpGetResponseEntityContentContinuator.cloneRequest(get);
        // we have to call getValue because most of the header classes dont actually equals themselves
        assertEquals(cloned.getFirstHeader(RANGE).getValue(), get.getFirstHeader(RANGE).getValue());

        cloned.setHeader(RANGE, new BoundedRequest(1, 2).render());
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

        // allow the Content-Range to be inferred from just the Content-Length
        final HttpDownloadContinuationMarker marker = HttpDownloadContinuationMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(request),
                SC_OK,
                extractDownloadResponseFingerprint(response, true));

        // passes with all stubs
        new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, marker, INFINITE_CONTINUATIONS);

        // basic null checks
        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(null, null, null, INFINITE_CONTINUATIONS, null));


        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, null, marker, INFINITE_CONTINUATIONS));

        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, null, INFINITE_CONTINUATIONS));

        // this connectionContext returns null for the getHttpClient call
        final MantaApacheHttpClientContext badConnCtx = mock(MantaApacheHttpClientContext.class);
        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(badConnCtx, request, marker, INFINITE_CONTINUATIONS));

        // this connectionContext somehow doesn't support retry cancellation
        // e.g. a custom MantaConnectionFactory or MantaConnectionContext was supplied
        final MantaApacheHttpClientContext retryNotCancellableConnCtx = mock(MantaApacheHttpClientContext.class);
        when(retryNotCancellableConnCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        when(retryNotCancellableConnCtx.isRetryEnabled()).thenReturn(true);
        when(retryNotCancellableConnCtx.isRetryCancellable()).thenReturn(false);

        assertThrows(HttpDownloadContinuationException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(retryNotCancellableConnCtx,
                                                                             request,
                                                                             marker,
                                                                             INFINITE_CONTINUATIONS));
    }

    @Test(expectedExceptions = UnknownHostException.class,
          expectedExceptionsMessageRegExp = ".*custom fatal exception yo.*")
    public void rethrowsFatalExceptions() throws Exception {
        final MantaApacheHttpClientContext connCtx = mock(MantaApacheHttpClientContext.class);
        when(connCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        final HttpGet request = new HttpGet();
        final HttpResponse response =
                prepareResponseWithHeaders(
                        unmodifiableMap(
                                ETAG, singleValueHeaderList(ETAG, "a"),
                                CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, "1")));
        when(response.getStatusLine()).thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, SC_PARTIAL_CONTENT, ""));

        final HttpDownloadContinuationMarker marker = HttpDownloadContinuationMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(request),
                SC_OK,
                extractDownloadResponseFingerprint(response, true));

        new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, marker, INFINITE_CONTINUATIONS)
                .buildContinuation(new UnknownHostException("custom fatal exception yo"), 0);
    }

    /**
     * There are a lot of combinations for ValidatesInitialExchange and validateResponse sorry to cram them all in one
     * method.
     */
    public void validateResponseExpectsNonNullHintsAndResponseFingerprint() throws Exception {
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void ValidateResponseExpectsNonNullFingerprint() throws ProtocolException {
        STUB_CONTINUATOR.validateResponseWithMarker(null);
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void validateResponseExpectsBothResponseHeaders() throws ProtocolException {
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.nullPair());
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void validateResponseExpectsContentRangeHeader() throws ProtocolException {
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.of(STUB_ETAG, null));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void validateResponseExpectsETagHeader() throws ProtocolException {
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.of(null, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void validateResponseExpectsMatchingResponseRange() throws ProtocolException {
        final HttpRange.Response badContentRange = new HttpRange.Response(0,
                                                                          STUB_CONTENT_LENGTH,
                                                                          STUB_CONTENT_LENGTH + 1);
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.of(STUB_ETAG, badContentRange));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void validateResponseExpectsMatchingETag() throws ProtocolException {
        final String badEtag = STUB_ETAG.substring(1);
        STUB_CONTINUATOR.validateResponseWithMarker(ImmutablePair.of(badEtag, STUB_CONTENT_RANGE));
    }

    public void buildContinuationSucceedsWhenAllStubsArePassed() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        final InputStream response =
                new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                        .buildContinuation(new IOException(), 0);

        assertEquals(IOUtils.toByteArray(response), STUB_CONTENT);
    }

    public void buildContinuationHandlesEofReadFailureGracefully() throws IOException {
        // ssshhh... we're not actually going to execute a request
        final HttpClient client = prepareMockedClient(0, null, null, null, null);

        final InputStream response =
                new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                        .buildContinuation(new IOException(), STUB_CONTENT.length);

        assertEquals(response.read(), -1);
    }

    @Test(expectedExceptions = HttpDownloadContinuationException.class,
          expectedExceptionsMessageRegExp = ".*update.*offset.*")
    public void buildContinuationInvalidBytesRead() throws IOException {
        // content-length agree with content-range but not the requested range
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), STUB_CONTENT.length + 1);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*response.*code.*")
    public void buildContinuationResponseInvalidResponseCode() throws IOException {
        final HttpClient client = prepareMockedClient(SC_OK,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*ETag.*missing.*")
    public void buildContinuationResponseMissingETagHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      null,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*ETag.*mismatch.*")
    public void buildContinuationResponseInvalidETagHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG.substring(1),
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Content-Range.*missing.*")
    public void buildContinuationResponseMissingContentRangeHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      null,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Content-Range.*Content-Length.*mismatch.*")
    public void buildContinuationResponseInvalidContentLengthForContentRangeHeader() throws IOException {
        // content-length agree with content-range but not the requested range
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      new HttpRange.Response(0, 1, 2),
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Entity.*missing.*")
    public void buildContinuationResponseEntityMissing() throws IOException {
        // entity is missing
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      null);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Entity content.*missing.*")
    public void buildContinuationResponseEntityContentMissing() throws IOException {
        // entity has no content
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      NoContentEntity.INSTANCE);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = HttpDownloadContinuationException.class,
          expectedExceptionsMessageRegExp = ".*woops.*")
    public void buildContinuationRethrowsExceptionsDuringExecute() throws IOException {
        final HttpClient client = mock(HttpClient.class);
        when(client.execute(any(HttpUriRequest.class), any(HttpContext.class))).thenThrow(new IOException("woops"));

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, null)
                .buildContinuation(new IOException(), 0);
    }

    public void recordsMetrics() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);
        final MetricRegistry registry = new MetricRegistry();

        final InputStreamContinuator continuator =
                new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, INFINITE_CONTINUATIONS, registry);

        final Optional<Histogram> maybeHistogram =
                registry.getHistograms((name, metric) -> name.equals(METRIC_NAME_CONTINUATIONS_PER_REQUEST))
                        .values().stream().findFirst();

        assertTrue(maybeHistogram.isPresent());

        final Histogram histogram = maybeHistogram.get();
        assertEquals(histogram.getCount(), 0);

        continuator.buildContinuation(new SocketTimeoutException(), 0);

        final String expectedMetricName =
                METRIC_NAME_RECOVERED_EXCEPTION_PREFIX + SocketTimeoutException.class.getSimpleName();
        final Optional<Counter> maybeSocketTimeoutExceptionCounter =
                registry.getCounters(
                        (name, meter) -> name.equals(expectedMetricName))
                        .values().stream().findFirst();

        assertTrue(maybeSocketTimeoutExceptionCounter.isPresent());

        final Counter socketTimeoutExceptionCounter = maybeSocketTimeoutExceptionCounter.get();

        assertEquals(socketTimeoutExceptionCounter.getCount(), 1);

        continuator.close();

        assertEquals(histogram.getCount(), 1);
    }

    private static HttpClient prepareMockedClient(final int responseCode,
                                                  final String etag,
                                                  final HttpRange.Response contentRange,
                                                  final Long contentLength,
                                                  final HttpEntity entity)
            throws IOException {
        final HttpClient client = mock(HttpClient.class);
        final AtomicBoolean responded = new AtomicBoolean(false);

        when(client.execute(any(HttpUriRequest.class), any(HttpContext.class))).then(invocation -> {
            if (!responded.compareAndSet(false, true)) {
                throw new IllegalStateException("this mocked client only provides a single response");
            }

            final HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, responseCode, "mocked");

            if (etag != null) {
                response.setHeader(ETAG, etag);
            }

            if (contentRange != null) {
                response.setHeader(CONTENT_RANGE, contentRange.render());
            }

            if (contentLength != null) {
                response.setHeader(CONTENT_LENGTH, contentLength.toString());
            }

            if (entity != null) {
                response.setEntity(entity);
            }

            return response;
        });

        return client;
    }
}
