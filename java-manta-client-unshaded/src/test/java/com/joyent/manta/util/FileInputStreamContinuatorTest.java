package com.joyent.manta.util;

import com.joyent.manta.util.FailingInputStream.FailureOrder;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;

import static java.lang.Math.floorDiv;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

@Test
public class FileInputStreamContinuatorTest {

    public void skipsCorrectly() throws Exception {
        final URI testFile = Thread.currentThread()
                .getContextClassLoader()
                .getResource("test-data/chaucer.txt")
                .toURI();

        final byte[] expectedContent = IOUtils.toByteArray(testFile);

        final File file = Paths.get(testFile).toFile();
        final FileInputStreamContinuator fileContinuator = new FileInputStreamContinuator(file);

        // we need to simulate a large download so we'll use a buffer size that's
        // smaller than the file
        final int copyBufferSize = toIntExact(floorDiv(file.length(), 3));

        // fail the copy halfway into the file
        final InputStream initialSource = new FailingInputStream(new FileInputStream(file),
                                                                 FailureOrder.PRE_READ,
                                                                 floorDiv(file.length(), 2));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        assertEquals(baos.size(), 0);

        final IOException e;
        try {
            IOUtils.copy(initialSource, baos, copyBufferSize);
            throw new AssertionError("should be unreachable");
        } catch (final IOException ioe) {
            e = ioe;
        }

        final int initialBytesRead = baos.size();
        assertNotEquals(initialBytesRead, 0);

        final InputStream continuation = fileContinuator.buildContinuation(e, baos.size());
        final long finalBytesRead = IOUtils.copy(continuation, baos, copyBufferSize);

        assertEquals(initialBytesRead + finalBytesRead, baos.size());
        assertEquals(file.length(), baos.size());

        // compare contents
        assertEquals(baos.toByteArray(), expectedContent);
    }

    public void testEOFFailure() throws Exception {
        final URI testFile = Thread.currentThread()
                .getContextClassLoader()
                .getResource("test-data/chaucer.txt")
                .toURI();

        final byte[] expectedContent = IOUtils.toByteArray(testFile);

        final File file = Paths.get(testFile).toFile();
        final FileInputStreamContinuator fileContinuator = new FileInputStreamContinuator(file);

        final int copyBufferSize = toIntExact(floorDiv(file.length(), 3));

        // fail on the EOF read
        final InputStream initialSource = new FailingInputStream(new FileInputStream(file),
                                                                 FailureOrder.ON_EOF,
                                                                 -1);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        assertEquals(baos.size(), 0);

        final IOException e;
        try {
            IOUtils.copy(initialSource, baos, copyBufferSize);
            throw new AssertionError("should be unreachable");
        } catch (final IOException ioe) {
            e = ioe;
        }

        final int initialBytesRead = baos.size();
        assertNotEquals(initialBytesRead, 0);
        assertEquals(initialBytesRead, file.length());

        final InputStream continuation = fileContinuator.buildContinuation(e, baos.size());
        final long finalBytesRead = IOUtils.copy(continuation, baos, copyBufferSize);

        assertEquals(initialBytesRead + finalBytesRead, baos.size());
        assertEquals(file.length(), baos.size());

        // compare contents
        assertEquals(baos.toByteArray(), expectedContent);

    }
}
