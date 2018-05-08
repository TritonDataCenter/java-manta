package com.joyent.manta.http;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class InstrumentedMantaHttpRequestExecutorTest {

    private static final String HTTP_METHOD_GET = "GET";

    private static final String HTTP_METHOD_PUT = "PUT";

    private static final Map<String, HttpResponse> RESPONSE_HEADER_SUCCESS;

    static {
        final CaseInsensitiveMap<String, HttpResponse> successCodeMap = new CaseInsensitiveMap<>();
        successCodeMap.put(
                HTTP_METHOD_GET,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, "OK")));
        successCodeMap.put(
                HTTP_METHOD_PUT,
                new BasicHttpResponse(new BasicStatusLine(HttpVersion.HTTP_1_1, HttpStatus.SC_NO_CONTENT, "No content")));
        RESPONSE_HEADER_SUCCESS = Collections.unmodifiableMap(successCodeMap);
    }

    @BeforeMethod
    public void setUp() throws Exception {
    }

    public void createGetRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_GET);
    }

    public void createPutRequestMetric() throws Exception {
        createsRequestMetric(HTTP_METHOD_PUT);
    }

    private void createsRequestMetric(final String method) throws Exception {
        final HttpRequest request;

        switch (method) {
            case HTTP_METHOD_GET:
                request = new HttpGet();
                break;
            case HTTP_METHOD_PUT:
                request = new HttpPut();
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
                registry.getTimers(MetricFilter.startsWith(method.toLowerCase()))
                        .values()
                        .stream()
                        .findFirst();

        assertTrue(maybeGetTimer.isPresent());
        assertEquals(maybeGetTimer.get().getCount(), 1, "get timer should have one sample");

        reqExec.execute(request, conn, ctx);
        assertEquals(maybeGetTimer.get().getCount(), 2, "get timer should have two samples");
    }

    private void createsExceptionMetric(final Class<? extends Exception>) {

    }

}
