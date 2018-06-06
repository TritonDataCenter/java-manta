package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.Response;
import org.apache.commons.text.RandomStringGenerator;
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

    public void cantValidateResponsesBeforeFirstUpdate() {
        final Response fullRange = new Response(0, 3, 4L);
        final ResumableDownloadMarker marker = new ResumableDownloadMarker("a", fullRange);

        assertThrows(NullPointerException.class, () -> marker.validateResponseRange(fullRange));
    }

    public void canValidateResponses() {
        final Response fullRange = new Response(0, 3, 4L);

        final ResumableDownloadMarker marker = new ResumableDownloadMarker("a", fullRange);

        // pretend we got the first byte
        marker.updateBytesRead(1);

        final Response validPartialRange = new Response(1, 3, 4L);
        marker.validateResponseRange(validPartialRange);

        final Response invalidEndRange = new Response(0, 2, 4L);
        assertThrows(IllegalArgumentException.class, () -> marker.validateResponseRange(invalidEndRange));

        // getting bytes we've already gotten is also bad
        assertThrows(IllegalArgumentException.class, () -> marker.validateResponseRange(fullRange));
    }

    public void testToString() {
        final Response fullRange = new Response(0, 3, 4L);
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final String marker = new ResumableDownloadMarker(etag, fullRange).toString();

        assertTrue(marker.contains(etag) && marker.contains(fullRange.toString()));
    }
}
