package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.Response;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

@Test
public class ResumableDownloadMarkerTest {

    public void ctorRejectsInvalidInputs() {
        final Response validRange = new Response(0, 1, 2L);

        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, null));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, validRange));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker("", validRange));
        assertThrows(NullPointerException.class, () -> new ResumableDownloadMarker("A", null));
    }

    public void testUpdateStart() {
    }

    public void testToString() {
    }
}
