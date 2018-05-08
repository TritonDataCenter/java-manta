package com.joyent.manta.http;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Verifies that request timing and exception count metrics are logged.
 * Request metrics are named "requests-$METHOD" while exception metrics are named "exceptions-$CLASS".
 */
@Test
public class InstrumentedMantaHttpRequestExecutorTest {

    private static final String HTTP_METHOD_HEAD = "HEAD";

    private static final String HTTP_METHOD_GET = "GET";

    private static final String HTTP_METHOD_PUT = "PUT";

    private static final String HTTP_METHOD_DELETE = "DELETE";

    private static final Map<String, HttpResponse> RESPONSE_HEADER_SUCCESS;

    static {
        final CaseInsensitiveMap<String, HttpResponse> successCodeMap = new CaseInsensitiveMap<>();
        successCodeMap.put(
                HTTP_METHOD_HEAD,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")));
        successCodeMap.put(
                HTTP_METHOD_GET,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")));
        successCodeMap.put(
                HTTP_METHOD_PUT,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, "No Content")));
        successCodeMap.put(
                HTTP_METHOD_DELETE,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, "No Content")));
        RESPONSE_HEADER_SUCCESS = Collections.unmodifiableMap(successCodeMap);
    }

    public void createHeadRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_HEAD);
    }

    public void createGetRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_GET);
    }

    public void createPutRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_PUT);
    }

    public void createDeleteRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_DELETE);
    }

    public void createSocketTimeoutExceptionMetricNoCausalChain() throws Exception {
        createsExceptionMetric(new SocketTimeoutException(), SocketTimeoutException.class);
    }

    public void createSocketTimeoutExceptionMetricEvenWhenWrapped() throws Exception {
        createsExceptionMetric(new MantaIOException(new SocketTimeoutException()), SocketTimeoutException.class);
    }

    private void createsRequestMetric(final String method) throws Exception {
        if (!RESPONSE_HEADER_SUCCESS.containsKey(method)) {
            throw new NotImplementedException("response is missing for method " + method);
        }

        final HttpRequest request;

        switch (method) {
            case HTTP_METHOD_HEAD:
                request = new HttpHead();
                break;
            case HTTP_METHOD_GET:
                request = new HttpGet();
                break;
            case HTTP_METHOD_PUT:
                request = new HttpPut();
                break;
            case HTTP_METHOD_DELETE:
                request = new HttpDelete();
                break;
            default:
                throw new NotImplementedException(method);
        }

        final MetricRegistry registry = new MetricRegistry();
        final HttpClientConnection conn = mock(HttpClientConnection.class);
        final HttpContext ctx = mock(HttpContext.class);

        when(conn.receiveResponseHeader()).thenReturn(RESPONSE_HEADER_SUCCESS.get(method));

        final InstrumentedMantaHttpRequestExecutor reqExec = new InstrumentedMantaHttpRequestExecutor(registry);

        reqExec.execute(request, conn, ctx);
        final Optional<Timer> maybeGetTimer =
                registry.getTimers(MetricFilter.contains("requests-" + method.toLowerCase()))
                        .values()
                        .stream()
                        .findFirst();

        assertTrue(maybeGetTimer.isPresent());
        assertEquals(maybeGetTimer.get().getCount(), 1, method + " timer should have one sample");

        reqExec.execute(request, conn, ctx);
        assertEquals(maybeGetTimer.get().getCount(), 2, method + " timer should have two samples");
    }

    private void createsExceptionMetric(final Exception ex,
                                        final Class<? extends Exception> exceptionRecorded) throws Exception {
        final MetricRegistry registry = new MetricRegistry();
        final HttpClientConnection conn = mock(HttpClientConnection.class);
        final HttpContext ctx = mock(HttpContext.class);

        when(conn.receiveResponseHeader()).thenThrow(ex);

        final InstrumentedMantaHttpRequestExecutor reqExec = new InstrumentedMantaHttpRequestExecutor(registry);

        Exception caughtEx = null;
        try {
            reqExec.execute(new HttpGet(), conn, ctx);
        } catch (final HttpException | IOException e) {
            caughtEx = e;
        }

        Assert.assertNotNull(caughtEx);

        final String exceptionName = exceptionRecorded.getSimpleName();
        final Optional<Meter> maybeExMeter =
                registry.getMeters(MetricFilter.contains("exceptions-" + exceptionName))
                        .values()
                        .stream()
                        .findFirst();

        assertTrue(maybeExMeter.isPresent());
        assertEquals(maybeExMeter.get().getCount(), 1, exceptionName + " meter should have one sample");
    }

}
