package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.ClosedInputStream;
import org.apache.commons.io.input.ProxyInputStream;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;

@Test
public class AutoContinuingInputStreamTest {

    /**
     * We can't use {@link org.apache.commons.io.input.BrokenInputStream} for this test because it will return the same
     * exception during close and trigger the
     * <a href="https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html#suppressed-exceptions">self-suppression
     * issue</a>.
     * <p>
     * On the other hand, we can't use {@link FailingInputStream} because that generates its own exceptions, so we
     * wouldn't be able to use {@link org.testng.Assert#assertSame} and check that no suppressed exceptions were added.
     */
    private static final class ReadExceptionInputStream extends ProxyInputStream {

        private final IOException exception;

        public ReadExceptionInputStream(final IOException exception) {
            super(ClosedInputStream.CLOSED_INPUT_STREAM);
            this.exception = exception;
        }

        @Override
        protected void beforeRead(final int n) throws IOException {
            throw this.exception;
        }
    }

    public void rethrowsUnrecoverableExceptionsDirectly() throws Exception {
        // the exception to consider fatal
        final IOException ex = new IOException("oops");

        // source stream always throws that exception
        final InputStream original = new ReadExceptionInputStream(ex);

        // pretend that it was a fatal exception and should be rethrown
        final InputStreamContinuator continuator = mock(InputStreamContinuator.class);
        when(continuator.buildContinuation(same(ex), anyLong())).thenThrow(ex);

        final IOException caught = expectThrows(IOException.class, () -> {
            try (final AutoContinuingInputStream in = new AutoContinuingInputStream(original, continuator)) {
                IOUtils.toByteArray(in);
            }
        });

        assertSame(caught, ex);
        assertEquals(caught.getSuppressed().length, 0);
    }
}
