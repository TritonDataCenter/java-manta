package com.joyent.manta.http;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

@Test
public class HttpRangeTest {

    public void ctorRejectsInvalidInputs() {
        // byte ranges are inclusive, this should fail
        assertThrows(IllegalStateException.class, () -> new HttpRange(0, 1, 1));
        // consequently, this should succeed
        new HttpRange(0, 1, 2);

        assertThrows(IllegalStateException.class, () -> new HttpRange(1, 0, 2));

        // requesting the second byte of a 1 byte file is invalid
        assertThrows(IllegalStateException.class, () -> new HttpRange(1, 1, 1));
    }

    public void parseRejectsInvalidInputs() {
        assertThrows(NullPointerException.class, () -> HttpRange.parseContentRange(null));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange(""));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("bytes"));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("bytes0-1/2"));
        assertThrows(IllegalArgumentException.class, () -> HttpRange.parseContentRange("byes 0-1/2"));
    }

    public void equalsWorks() {
        final HttpRange twoBytes = new HttpRange(0, 1, 2);

        assertEquals(twoBytes, twoBytes);
        assertEquals(twoBytes, new HttpRange(0, 1, 2));
        assertNotEquals(twoBytes, new HttpRange(0, 2, 3));
    }


}
