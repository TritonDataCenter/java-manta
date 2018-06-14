package com.joyent.manta.http;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static com.joyent.manta.util.UnitTestConstants.UNIT_TEST_URL;
import static org.apache.http.HttpHeaders.RANGE;
import static org.apache.http.HttpStatus.SC_NOT_IMPLEMENTED;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.apache.http.HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE;
import static org.apache.http.HttpVersion.HTTP_1_1;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

@Test
public class FixedFailureCountByteArrayObjectHttpHandlerTest {

    public void complainsAboutInvalidRequestRanges() throws Exception {
        final int size = 5;
        final int startInclusive = 0;
        final int endInclusive = size - 1;
        final byte[] object = new byte[size];
        for (byte c = 0; c < object.length; c++) {
            object[c] = (byte) ('a' + c);
        }

        final List<Header> requestRangeHeaders = Arrays.asList(
                rangeHeader(startInclusive - 1, endInclusive),
                rangeHeader(startInclusive, endInclusive + 1),
                rangeHeader(startInclusive - 1, endInclusive + 1),
                rangeHeader(endInclusive, startInclusive));

        for (final Header requestRange : requestRangeHeaders) {
            final HttpRequest req = new HttpGet(UNIT_TEST_URL);
            req.setHeader(requestRange);

            final HttpResponse res = new BasicHttpResponse(HTTP_1_1, SC_NOT_IMPLEMENTED, "Sorry");
            final HttpContext ctx = new BasicHttpContext();
            final FixedFailureCountByteArrayObjectHttpHandler handler = new FixedFailureCountByteArrayObjectHttpHandler(object, 0);
            handler.handle(req, res, ctx);
            assertThrows(NullPointerException.class, () -> handler.populateEntity(res));

            assertEquals(
                    res.getStatusLine().getStatusCode(),
                    SC_REQUESTED_RANGE_NOT_SATISFIABLE,
                    String.format("Unexpected response code for header: %s", requestRange));

            assertNull(res.getEntity(), String.format("Unexpected entity returned for header: %s", requestRange));
        }
    }

    public void canDeliverSingleByteRangeOfLargerObject() throws Exception {
        final byte[] object = {'a', 'b', 'c'};

        for (int i = 0; i < object.length; i++) {
            canDeliverArbitraryByteRangesOfLargerObject(object, i, i);
        }
    }

    public void canDeliverLargerByteRanges() throws Exception {
        final byte[] object = {'a', 'b', 'c', 'd', 'e'};

        for (int start = 0; start < object.length; start++) {
            for (int end = start; end < object.length; end++) {
                canDeliverArbitraryByteRangesOfLargerObject(object, start, end);
            }
        }
    }

    private static void canDeliverArbitraryByteRangesOfLargerObject(final byte[] object,
                                                                    final int rangeStart,
                                                                    final int rangeEnd) throws Exception {
        final HttpRequest req = new HttpGet(UNIT_TEST_URL);
        req.setHeader(rangeHeader(rangeStart, rangeEnd));

        final HttpResponse res = new BasicHttpResponse(HTTP_1_1, SC_NOT_IMPLEMENTED, "Sorry");
        final HttpContext ctx = new BasicHttpContext();
        final FixedFailureCountByteArrayObjectHttpHandler handler = new FixedFailureCountByteArrayObjectHttpHandler(object, 0);

        handler.handle(req, res, ctx);
        handler.populateEntity(res);

        assertEquals(res.getStatusLine().getStatusCode(), SC_PARTIAL_CONTENT);

        final HttpEntity resEntity = res.getEntity();
        assertNotNull(resEntity);

        final InputStream resContent = resEntity.getContent();
        assertNotNull(resContent);

        final byte[] received = IOUtils.toByteArray(resContent);
        assertEquals(received.length, 1 + rangeEnd - rangeStart);

        assertEquals(ArrayUtils.subarray(object, rangeStart, rangeEnd + 1), received);
    }

    private static Header rangeHeader(final long startInclusive, final long endInclusive) {
        return new BasicHeader(RANGE, String.format("bytes=%d-%d", startInclusive, endInclusive));
    }
}
