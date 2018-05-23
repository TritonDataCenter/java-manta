package com.joyent.manta.http;

import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

@Test
public class ContentRangeTest {

    public void ctorRejectsInvalidInputs() {
        // byte ranges are inclusive, this should fail
        assertThrows(IllegalStateException.class, () -> new ContentRange(0, 1, 1));
        // consequently, this should succeed
        new ContentRange(0, 1, 2);

        assertThrows(IllegalStateException.class, () -> new ContentRange(1, 0, 2));

        // requesting the second byte of a 1 byte file is invalid
        assertThrows(IllegalStateException.class, () -> new ContentRange(1, 1, 1));
    }

    public void parseRejectsInvalidInputs() {
        assertThrows(NullPointerException.class, () -> ContentRange.parse(null));
        assertThrows(IllegalArgumentException.class, () -> ContentRange.parse(""));
        assertThrows(IllegalArgumentException.class, () -> ContentRange.parse("bytes"));
        assertThrows(IllegalArgumentException.class, () -> ContentRange.parse("bytes0-1/2"));
        assertThrows(IllegalStateException.class, () -> ContentRange.parse("byes 0-1/2"));
    }
}
