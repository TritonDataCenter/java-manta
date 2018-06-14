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

    public void canValidateResponses() throws HttpException {
        final Response fullRange = new Response(0, 3, 4L);

        final ResumableDownloadMarker marker = new ResumableDownloadMarker("a", fullRange);

        // pretend there was an error receiving headers and we had to restart the initial request completely
        marker.validateRange(fullRange);

        // pretend we got the first byte
        marker.updateBytesRead(1);

        final Response validPartialRange = new Response(1, 3, 4L);
        marker.validateRange(validPartialRange);

        final Response invalidEndRange = new Response(0, 2, 4L);
        assertThrows(HttpException.class, () -> marker.validateRange(invalidEndRange));

        // getting bytes we've already gotten is also bad
        assertThrows(HttpException.class, () -> marker.validateRange(fullRange));
    }

    public void usefulToString() {
        final Response fullRange = new Response(0, 3, 4L);
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final String rangeRgx = StringUtils.join(
                new Object[]{"", fullRange.getStartInclusive(), fullRange.getEndInclusive(), ""},
                ".*");

        final String marker = new ResumableDownloadMarker(etag, fullRange).toString();

        assertTrue(marker.contains(etag));
        assertTrue(marker.matches(rangeRgx));
    }
}
