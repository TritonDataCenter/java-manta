package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.HttpException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class ResumableDownloadMarkerTest {

    public void ctorRejectsInvalidInputs() {
        final Response validRange = new Response(0, 1, 2L);

        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, null));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, validRange));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker("", validRange));
        assertThrows(NullPointerException.class, () -> new ResumableDownloadMarker("A", null));
    }

    private static final String STUB_ETAG = "abc";

    private static final Response STUB_CONTENT_RANGE = new Response(0, 3, 4L);

    public void canValidateResponses() throws HttpException {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // pretend there was an error receiving headers and we had to restart the initial request completely
        marker.validateResponseRange(STUB_CONTENT_RANGE);

        // pretend we got the first byte
        marker.updateRangeStart(1);

        final Response validPartialRange = new Response(1, 3, 4L);
        marker.validateResponseRange(validPartialRange);

        final Response invalidEndRange = new Response(0, 2, 4L);
        assertThrows(HttpException.class, () -> marker.validateResponseRange(invalidEndRange));

        // getting bytes we've already gotten is also bad
        assertThrows(HttpException.class, () -> marker.validateResponseRange(STUB_CONTENT_RANGE));
    }

    public void validatesBytesRead() {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // it's possible to encounter an error without reading any bytes
        marker.updateRangeStart(0);

        // shouldn't read a negative number of bytes
        assertThrows(IllegalArgumentException.class,
                     () -> marker.updateRangeStart(-1));

        // shouldn't be able to read more bytes than the total object size
        assertThrows(IllegalArgumentException.class,
                     () -> marker.updateRangeStart(10));

        // advance marker by having read two bytes
        marker.updateRangeStart(2);

        // can't have read less bytes than we did at last update
        assertThrows(IllegalArgumentException.class,
                     () -> marker.updateRangeStart(1));


        // TODO: how do we handle this case?
        assertThrows(IllegalArgumentException.class,
                     () -> marker.updateRangeStart(4));

    }

    public void usefulToString() {
        final Response STUB_FULL_RANGE = new Response(0, 3, 4L);
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final String rangeRgx = StringUtils.join(
                new Object[]{"", STUB_FULL_RANGE.getStartInclusive(), STUB_FULL_RANGE.getEndInclusive(), ""},
                ".*");

        final String marker = new ResumableDownloadMarker(etag, STUB_FULL_RANGE).toString();

        assertTrue(marker.contains(etag));
        assertTrue(marker.matches(rangeRgx));
    }
}
