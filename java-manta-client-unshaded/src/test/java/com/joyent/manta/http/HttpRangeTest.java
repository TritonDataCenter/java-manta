package com.joyent.manta.http;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

@Test
public class HttpRangeTest {

    public void baseCtorRejectsInvalidInputs() {
        new HttpRange(0, 1, 2L);

        // byte ranges are inclusive, these should fail
        assertThrows(IllegalStateException.class, () -> new HttpRange(1, 0, null));
        assertThrows(IllegalStateException.class, () -> new HttpRange(0, 1, 0L));
        assertThrows(IllegalStateException.class, () -> new HttpRange(0, 1, 1L));
        assertThrows(IllegalStateException.class, () -> new HttpRange(1, 0, 2L));

        // a single byte is not forbidden
        new HttpRange(0, 0, 1L);
    }

    public void requestCtorRejectsInvalidInputs() {
        new HttpRange.Request(0, 1);

        assertThrows(IllegalStateException.class, () -> new HttpRange.Request(1, 0));

        // requesting a single byte is not forbidden
        new HttpRange.Request(0, 0);
    }

    public void parseRejectsInvalidInputs() {
        assertThrows(NullPointerException.class, () -> HttpRange.parseContentRange(null));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange(""));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("bytes"));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("bytes0-1/2"));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("byes 0-1/2"));
    }

    public void baseEqualsWorks() {
        final HttpRange twoBytes = new HttpRange(0, 1, 2L);

        assertEquals(twoBytes, twoBytes);
        assertEquals(twoBytes, new HttpRange(0, 1, 2L));
        assertNotEquals(twoBytes, new HttpRange(0, 2, 3L));
    }
}
