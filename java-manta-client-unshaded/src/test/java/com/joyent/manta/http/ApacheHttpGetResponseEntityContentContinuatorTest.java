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
import com.joyent.manta.exception.ResumableDownloadException;
import com.joyent.manta.exception.ResumableDownloadUnexpectedResponseException;
import com.joyent.manta.http.entity.NoContentEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.METRIC_NAME_CONTINUATIONS_PER_REQUEST;
import static com.joyent.manta.http.ApacheHttpGetResponseEntityContentContinuator.METRIC_NAME_PREFIX_METER_RECOVERED;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class ApacheHttpGetResponseEntityContentContinuatorTest {

    public void canCloneHttpGet() {

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

        // allow the Content-Range to be inferred from just the Content-Length
        final ResumableDownloadMarker marker = ResumableDownloadMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(request),
                extractDownloadResponseFingerprint(response, true));

        // passes with all stubs
        new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, marker);

        // basic null checks
        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(null, null, null, null));


        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, null, marker));

        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, null));

        // this connectionContext returns null for the getHttpClient call
        final MantaApacheHttpClientContext badConnCtx = mock(MantaApacheHttpClientContext.class);
        assertThrows(NullPointerException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(badConnCtx, request, marker));

        // this connectionContext somehow doesn't support retry cancellation
        // e.g. a custom MantaConnectionFactory or MantaConnectionContext was supplied
        final MantaApacheHttpClientContext retryNotCancellableConnCtx = mock(MantaApacheHttpClientContext.class);
        when(retryNotCancellableConnCtx.getHttpClient()).thenReturn(mock(CloseableHttpClient.class));
        when(retryNotCancellableConnCtx.isRetryEnabled()).thenReturn(true);
        when(retryNotCancellableConnCtx.isRetryCancellable()).thenReturn(false);

        assertThrows(ResumableDownloadException.class,
                     () -> new ApacheHttpGetResponseEntityContentContinuator(retryNotCancellableConnCtx,
                                                                             request,
                                                                             marker));
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

        final ResumableDownloadMarker marker = ResumableDownloadMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(request),
                extractDownloadResponseFingerprint(response, true));

        new ApacheHttpGetResponseEntityContentContinuator(connCtx, request, marker)
                .buildContinuation(new UnknownHostException("custom fatal exception yo"), 0);
    }

    public void createMarkerValidatesHints() throws Exception {
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        // any returned range is fine since none was specified
        final HttpRange.Response range = new HttpRange.Response(0, 1, 2L);

        // invalid ETag
        final String badEtag = etag.substring(1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                                                                           ImmutablePair.of(null, null)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                                                                           ImmutablePair.of(null, range)));

        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> ResumableDownloadMarker.validateInitialExchange(
                             ImmutablePair.of(etag, null), ImmutablePair.of(badEtag, range)));

        assertNotNull(
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(etag, null),
                                                                ImmutablePair.of(etag, range)));
    }

    public void validateResponseExpectsNonNullHintsAndResponseFingerprint() throws Exception {
        final HttpClient client = mock(HttpClient.class);
        final HttpGet req = new HttpGet();

        final String etag = "abc";
        final long contentLength = 2;
        final HttpRange.Response contentRange = fromContentLength(HttpRange.Response.class, contentLength);
        final HttpResponse res = prepareResponseWithHeaders(
                unmodifiableMap(
                        ETAG, singleValueHeaderList(ETAG, etag),
                        CONTENT_LENGTH, singleValueHeaderList(CONTENT_LENGTH, Long.toString(contentLength)),
                        CONTENT_RANGE, singleValueHeaderList(CONTENT_RANGE, contentRange.render())));

        final ResumableDownloadMarker marker = ResumableDownloadMarker.validateInitialExchange(
                extractDownloadRequestFingerprint(req),
                // don't allow the Content-Range to be inferred by passing false
                extractDownloadResponseFingerprint(res, false));

        final ApacheHttpGetResponseEntityContentContinuator continuator =
                new ApacheHttpGetResponseEntityContentContinuator(client, req, marker, null);

        continuator.validateResponseWithMarker(ImmutablePair.of(etag, contentRange));

        // the following assertion just tests for programmer error
        assertThrows(NullPointerException.class,
                     () -> continuator.validateResponseWithMarker(null));

        // the following assertions test a response with insufficient headers
        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.nullPair()));
        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.of(etag, null)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.of(null, contentRange)));

        // the following assertions test a response with incorrect headers
        final String badEtag = etag.substring(1);
        final HttpRange.Response badContentRange = new HttpRange.Response(0, contentLength, contentLength + 1);

        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.of(etag, badContentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.of(badEtag, contentRange)));
        assertThrows(ResumableDownloadUnexpectedResponseException.class,
                     () -> continuator.validateResponseWithMarker(ImmutablePair.of(badEtag, badContentRange)));
    }

    private static final byte[] STUB_CONTENT = new byte[]{'f', 'o', 'o',};

    // for simplicity the etag is the same as the content
    private static final String STUB_ETAG = new String(STUB_CONTENT, StandardCharsets.US_ASCII);

    private static final HttpRange.Response STUB_CONTENT_RANGE = new HttpRange.Response(0, 2, 3);

    private static final ByteArrayEntity STUB_RESPONSE_ENTITY = new ByteArrayEntity(STUB_CONTENT);

    private static final ResumableDownloadMarker STUB_MARKER = new ResumableDownloadMarker(STUB_ETAG,
                                                                                           STUB_CONTENT_RANGE);

    public void buildContinuationSucceedsWhenAllStubsArePassed() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        final InputStream response =
                new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                        .buildContinuation(new IOException(), 0);

        assertEquals(IOUtils.toByteArray(response), STUB_CONTENT);
    }

    @Test(expectedExceptions = ResumableDownloadException.class,
          expectedExceptionsMessageRegExp = ".*update.*offset.*")
    public void buildContinuationInvalidBytesRead() throws IOException {
        // content-length agree with content-range but not the requested range
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), STUB_CONTENT.length);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*response.*code.*")
    public void buildContinuationResponseInvalidResponseCode() throws IOException {
        final HttpClient client = prepareMockedClient(SC_OK,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*ETag.*missing.*")
    public void buildContinuationResponseMissingETagHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      null,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*ETag.*mismatch.*")
    public void buildContinuationResponseInvalidETagHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      "abc",
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Content-Range.*missing.*")
    public void buildContinuationResponseMissingContentRangeHeader() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      null,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Content-Range.*Content-Length.*mismatch.*")
    public void buildContinuationResponseInvalidContentLengthForContentRangeHeader() throws IOException {
        // content-length agree with content-range but not the requested range
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      new HttpRange.Response(0, 1, 2),
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Entity.*missing.*")
    public void buildContinuationResponseEntityMissing() throws IOException {
        // entity is missing
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      null);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadUnexpectedResponseException.class,
          expectedExceptionsMessageRegExp = ".*Entity content.*missing.*")
    public void buildContinuationResponseEntityContentMissing() throws IOException {
        // entity has no content
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      NoContentEntity.INSTANCE);

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
    }

    @Test(expectedExceptions = ResumableDownloadException.class,
          expectedExceptionsMessageRegExp = ".*woops.*")
    public void buildContinuationRethrowsExceptionsDuringExecute() throws IOException {
        final HttpClient client = mock(HttpClient.class);
        when(client.execute(any(HttpUriRequest.class), any(HttpContext.class))).thenThrow(new IOException("woops"));

        new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, null)
                .buildContinuation(new IOException(), 0);
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

    public void recordsMetrics() throws IOException {
        final HttpClient client = prepareMockedClient(SC_PARTIAL_CONTENT,
                                                      STUB_ETAG,
                                                      STUB_CONTENT_RANGE,
                                                      (long) STUB_CONTENT.length,
                                                      STUB_RESPONSE_ENTITY);
        final MetricRegistry registry = new MetricRegistry();

        final InputStreamContinuator continuator =
                new ApacheHttpGetResponseEntityContentContinuator(client, new HttpGet(), STUB_MARKER, registry);

        final Optional<Histogram> maybeHistogram =
                registry.getHistograms((name, metric) -> name.equals(METRIC_NAME_CONTINUATIONS_PER_REQUEST))
                        .values().stream().findFirst();

        assertTrue(maybeHistogram.isPresent());

        final Histogram histogram = maybeHistogram.get();
        assertEquals(histogram.getCount(), 0);

        continuator.buildContinuation(new SocketTimeoutException(), 0);

        final String expectedMetricName =
                METRIC_NAME_PREFIX_METER_RECOVERED + SocketTimeoutException.class.getSimpleName();
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
}
