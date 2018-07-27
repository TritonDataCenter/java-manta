package com.joyent.manta.util;

import org.testng.annotations.Test;

/**
 * The primary purpose of this test class is to verify that a continuator does not indefinitely consume resources.
 * Since the {@link AutoContinuingInputStream} class will retry indefinitely (the decision to decide when to give up
 * was delegated to the continuator
 */
public class AutoContinuingInputStreamTest {

    public AutoContinuingInputStream() {
    }

    private static class ByteArrayInputStreamContinuator {

    }

    @Test
    public void testReadDoesntRunIndefinitelyWithByteArrayInputStream() {
        final InputStreamContinuator byteContinuator = new ByteArrayInputStreamContinuator();
    }
}
