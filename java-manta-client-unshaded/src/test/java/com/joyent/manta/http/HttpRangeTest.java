package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.Request;
import com.joyent.manta.http.HttpRange.Response;
import org.apache.http.HttpException;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class HttpRangeTest {


    public void requestCtorRejectsInvalidInputs() {
        new Request(0, 1);

        assertThrows(IllegalArgumentException.class, () -> new Request(1, 0));

        // requesting a single byte is not forbidden
        new Request(0, 0);
    }

    public void responseCtorRejectsInvalidInputs() {
        new Response(0, 1, 2L);

        // byte ranges are inclusive, these should fail
        assertThrows(IllegalArgumentException.class, () -> new Response(0, 1, 0L));
        assertThrows(IllegalArgumentException.class, () -> new Response(0, 1, 1L));
        assertThrows(IllegalArgumentException.class, () -> new Response(1, 0, 2L));

        // a single byte is a valid range as well
        new Response(0, 0, 1L);
    }

    public void parseContentRangeRejectsInvalidInputs() {
        assertThrows(NullPointerException.class, () -> HttpRange.parseContentRange(null));
        assertThrows(HttpException.class, () -> HttpRange.parseContentRange(""));
        assertThrows(HttpException.class, () -> HttpRange.parseContentRange("bytes"));
        assertThrows(HttpException.class, () -> HttpRange.parseContentRange("bytes0-1/2"));
        assertThrows(HttpException.class, () -> HttpRange.parseContentRange("byes 0-1/2"));
    }

    public void requestCanValidateResponse() {
        final Request reqRange = new Request(0, 5);
        final Response contentRange = new Response(0, 5, 6);

        reqRange.matches(contentRange);

        final Response badStartContentRange = new Response(1, 5, 6);
        assertFalse(reqRange.matches(badStartContentRange));

        final Response badEndContentRange = new Response(0, 4, 6);
        assertFalse(reqRange.matches(badEndContentRange));
    }

    public void usefulToStringMethods() {
        final String req = new Request(0, 1).toString();
        final String res = new Response(2, 3, 4).toString();

        assertTrue(req.contains("startInclusive") && req.contains("0"));
        assertTrue(req.contains("endInclusive") && req.contains("1"));
        assertFalse(req.contains("size"));

        assertTrue(res.contains("startInclusive") && res.contains("2"));
        assertTrue(res.contains("endInclusive") && res.contains("3"));
        assertTrue(res.contains("size") && res.contains("4"));
    }

}
