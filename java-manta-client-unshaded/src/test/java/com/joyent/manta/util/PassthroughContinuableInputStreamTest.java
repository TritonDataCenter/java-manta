package com.joyent.manta.util;

import org.apache.commons.io.input.NullInputStream;
import org.testng.annotations.Test;

import static org.testng.Assert.assertThrows;

@Test
public class PassthroughContinuableInputStreamTest {


    public void testValidatesInputs() throws Exception {
        final PassthroughResumableInputStream mis = new PassthroughResumableInputStream();
        assertThrows(NullPointerException.class, () -> mis.setSource(null));

        assertThrows(NullPointerException.class, () -> mis.read());
        assertThrows(NullPointerException.class, () -> mis.read(null, 0, 0));

        // read 1 byte with space for 0 bytes
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[0], 0, 1));

        // read 0 bytes with invalid offset
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[1], -1, 0));

        // read more bytes than space available
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[1], 0, 2));

        // read more bytes than space available (due to offset)
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[2], 1, 2));

        final PassthroughResumableInputStream closed = new PassthroughResumableInputStream();
        closed.close();

        assertThrows(IllegalStateException.class, () -> closed.setSource(new NullInputStream(0)));

        assertThrows(IllegalStateException.class, () -> closed.read());

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1]));
    }


}
