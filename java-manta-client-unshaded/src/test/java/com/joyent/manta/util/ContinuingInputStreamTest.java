package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.toIntExact;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Test
public class ContinuingInputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuingInputStreamTest.class);

    @AfterMethod
    public void afterMethod() {
        Mockito.validateMockitoUsage();
    }

    public void testValidatesInputs() throws Exception {
        assertThrows(NullPointerException.class, () -> new ContinuingInputStream(null));

        final ContinuingInputStream cis = new ContinuingInputStream(
                new ByteArrayInputStream(RandomUtils.nextBytes(10)));

        assertThrows(NullPointerException.class, () -> cis.continueWith(null));

        final ContinuingInputStream closed = new ContinuingInputStream(new NullInputStream(1));
        closed.close();

        assertThrows(IllegalStateException.class, () -> closed.continueWith(new NullInputStream(0)));

        assertThrows(IllegalStateException.class, () -> closed.read());

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1], 0, 1));

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1]));
    }

    // theoretically this stream could delegate all reset/mark operations to the underlying stream
    // but we don't know if every wrapped stream will support these methods so mark/reset is currently out of scope
    public void testMarkResetInputStreamMethods() throws Exception {
        final ContinuingInputStream cis = new ContinuingInputStream(new NullInputStream(1));

        assertFalse(cis.markSupported());

        assertThrows(UnsupportedOperationException.class, () -> cis.mark(1));
        assertThrows(UnsupportedOperationException.class, () -> cis.reset());
    }

    public void testCloseClosesWrappedInputStream() throws Exception {
        final InputStream source = mock(InputStream.class);
        final ContinuingInputStream cis = new ContinuingInputStream(source);
        cis.close();

        verify(source).close();
    }

    public void testClosingWithoutSettingDoesNotThrow() throws Exception {
        final ContinuingInputStream cis = new ContinuingInputStream(new NullInputStream(1));

        cis.close();
        assertTrue(true);
    }

    public void testStreamsAreClosedWhereExpected() throws Exception {
        final InputStream initial = mock(InputStream.class);
        final ContinuingInputStream cis = new ContinuingInputStream(initial);

        cis.close();
        verify(initial, times(1)).close();
        verifyNoMoreInteractions(initial);

        // extra close call doesn't throw
        cis.close();
    }

    private static final byte[] STUB_OBJECT_BYTES = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',};

    private static final int EOF = -1;

    public void testSingleByteReads() throws Exception {
        final ContinuingInputStream cis = new ContinuingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES));
        final byte[] copy = new byte[STUB_OBJECT_BYTES.length];

        int b;
        int idx = 0;
        while ((b = cis.read()) != -1) {
            copy[idx++] = (byte) b;
        }

        assertArrayEquals(STUB_OBJECT_BYTES, copy);

        assertEquals(cis.read(), EOF);
        assertEquals(cis.read(new byte[1]), EOF);
        assertEquals(cis.read(new byte[1], 0, 1), EOF);
    }

    public void testReadFailureBeforeCopyingBytesDoesNotAffectCount() throws Exception {
        final InputStream immediatelyPreReadFailing = new FailingInputStream(
                new ByteArrayInputStream(STUB_OBJECT_BYTES), 1, false);

        // read will fail before any bytes are read
        final ContinuingInputStream cisSingle = new ContinuingInputStream(immediatelyPreReadFailing);
        assertThrows(IOException.class, () -> cisSingle.read());
        // no bytes were read
        assertEquals(cisSingle.getBytesRead(), 0);

        final ContinuingInputStream cisBuffer = new ContinuingInputStream(immediatelyPreReadFailing);
        assertThrows(IOException.class, () -> cisBuffer.read(new byte[1]));
        assertEquals(cisBuffer.getBytesRead(), 0);

        final ContinuingInputStream cisBufferOffLen = new ContinuingInputStream(immediatelyPreReadFailing);
        assertThrows(IOException.class, () -> cisBufferOffLen.read(new byte[1], 0, 1));
        assertEquals(cisBufferOffLen.getBytesRead(), 0);
    }

    public void testReadFailureAfterCopyingBytesDoesNotAffectCount() throws Exception {
        final InputStream immediatelyPostReadFailing = new FailingInputStream(
                new ByteArrayInputStream(STUB_OBJECT_BYTES), 1, true);

        // read will fail before any bytes are read
        final ContinuingInputStream cisSingle = new ContinuingInputStream(immediatelyPostReadFailing);
        assertThrows(IOException.class, () -> cisSingle.read());
        // no bytes were (confirmed to have been) read
        assertEquals(cisSingle.getBytesRead(), 0);

        final ContinuingInputStream cisBuffer = new ContinuingInputStream(immediatelyPostReadFailing);
        assertThrows(IOException.class, () -> cisBuffer.read(new byte[1]));
        assertEquals(cisBuffer.getBytesRead(), 0);

        final ContinuingInputStream cisBufferOffLen = new ContinuingInputStream(immediatelyPostReadFailing);
        assertThrows(IOException.class, () -> cisBufferOffLen.read(new byte[1], 0, 1));
        assertEquals(cisBufferOffLen.getBytesRead(), 0);
    }

    private static void testBufferedRead(final int objectSize,
                                         final int readOffset,
                                         final int readLength) throws IOException {
        final byte[] object = ArrayUtils.subarray(STUB_OBJECT_BYTES, 0, objectSize);

        final ContinuingInputStream cis = new ContinuingInputStream(new ByteArrayInputStream(object));
        final byte[] readBuffer = new byte[readLength];

        if (readBuffer.length < readOffset + readLength) {
            assertThrows(IndexOutOfBoundsException.class, () -> cis.read(readBuffer, readOffset, readLength));
            return;
        }

        final int bytesRead = cis.read(readBuffer, readOffset, readLength);

        assertEquals(bytesRead, cis.getBytesRead());

        assertEquals(bytesRead, Math.min(objectSize, readLength));

        final byte[] copied = ArrayUtils.subarray(readBuffer, readOffset, toIntExact(cis.getBytesRead()));

        final byte[] expected = ArrayUtils.subarray(STUB_OBJECT_BYTES, readOffset, Math.min(objectSize, readLength));
        assertArrayEquals(expected, copied);
    }

    public void testVariableObjectAndBufferSizesWithFullSingleBufferReads() throws Exception {
        for (int objectSize = 1; objectSize < STUB_OBJECT_BYTES.length; objectSize++) {
            for (int readOffset = 0; readOffset < STUB_OBJECT_BYTES.length; readOffset++) {
                for (int readLength = 1; readLength < STUB_OBJECT_BYTES.length; readLength++) {
                    try {
                        testBufferedRead(objectSize, readOffset, readLength);
                    } catch (final Throwable e) {
                        throw new AssertionError(
                                String.format(
                                        "failed testBufferedRead with inputs: objectSize=%d, readOffset=%d, readLength=%d",
                                        objectSize,
                                        readOffset,
                                        readLength),
                                e);
                    }
                }
            }
        }
    }

    public void testVariableStartOffsetAndFrequencyFailures() {

        final ArrayList<Object[]> paramLists = new ArrayList<>();
        final Set<Integer> deepHashcodes = new HashSet<>();

        // we want to test combinations of object and read buffer sizes
        for (int objectSize = 1; objectSize <= STUB_OBJECT_BYTES.length; objectSize++) {
            // we want to simulate failures at the final byte read that would produce EOF so we use <=, not <
            for (int readBufferSize = 1; readBufferSize <= STUB_OBJECT_BYTES.length; readBufferSize++) {
                // we want to make sure the number of bytes read is only updated if bytes are successfully delivered to the caller
                for (final FailureOrderType failureOrderType : FailureOrderType.values()) {
                    // i is the index of the failure being
                    for (int i = 0; i <= STUB_OBJECT_BYTES.length; i++) {
                        for (int failureCount = 0; failureCount < i; failureCount++) {
                            if (failureCount == 0) {
                                continue;
                            }

                            // for each count of failures being generated, also shift the entire set of failure offsets by an increasing amount
                            // that is up to 1 less than half of the inputSize
                            for (int failureGlobalOffset = 0; failureGlobalOffset < i; failureGlobalOffset++) {

                                final ArrayDeque<ImmutablePair<Integer, Boolean>> failureOffsets = new ArrayDeque<>();

                                final int divided = Math.floorDiv(i, failureCount);
                                final int failureSpacing = NumberUtils.max(1, divided);
                                Boolean failureOrder = null;

                                for (int failure = 0; failure <= failureCount; failure++) {
                                    final int failureOffset = (failure * failureSpacing) + failureGlobalOffset;
                                    if (i <= failureOffset) {
                                        // the test will complain if we pass in a failure offset that is equal to the inputSize
                                        // (since the array position at input[input.length] or after that don't make sense
                                        continue;
                                    }

                                    switch (failureOrderType) {
                                        case PRE_READ:
                                            failureOrder = false;
                                            break;
                                        case POST_READ:
                                            failureOrder = true;
                                            break;
                                        case PRE_READ_INITIAL_ALTERNATING:
                                            failureOrder = failureOrder == null ? false : failureOrder ^ true;
                                            break;
                                        case POST_READ_INITIAL_ALTERNATING:
                                            failureOrder = failureOrder == null ? true : failureOrder ^ true;
                                            break;
                                    }

                                    failureOffsets.add(ImmutablePair.of(failureOffset, failureOrder));
                                }

                                addToParamListIfUnseen(deepHashcodes, paramLists,
                                        new Object[]{objectSize, readBufferSize, failureOffsets});
                            }
                        }
                    }
                }
            }
        }

        int tests = 0;
        for (Object[] params : paramLists) {
            tests++;
            testBytesReadUpdatesReliably((Integer) params[0], (Integer) params[1],
                    (Deque<ImmutablePair<Integer, Boolean>>) params[2]);
        }

        LOG.debug("testVariableStartOffsetAndFrequencyFailures completed {} input combinations", tests);
    }

    private static void testBytesReadUpdatesReliably(final int objectSize,
                                                     final int readBufferSize,
                                                     final Deque<ImmutablePair<Integer, Boolean>> failureOffsets) {
        final byte[] object = ArrayUtils.subarray(STUB_OBJECT_BYTES, 0, objectSize);

        final ByteArrayOutputStream copied = new ByteArrayOutputStream();

        int infiniteLoopDetector = failureOffsets.size() + 2;

        assertFalse(failureOffsets.isEmpty());
        final List<ImmutablePair<Integer, Boolean>> failureOffsetsCopy = new ArrayList<>(failureOffsets);

        final Pair<Integer, Boolean> firstFailure = failureOffsets.removeFirst();
        final ContinuingInputStream continuable = new ContinuingInputStream(
                new FailingInputStream(
                        new ByteArrayInputStream(object),
                        firstFailure.getLeft(),
                        firstFailure.getRight()));

        boolean finished = false;
        do {
            if (--infiniteLoopDetector == 0) {
                throw new AssertionError(
                        String.format(
                                "looping indefinitely with inputs: %nreadBufferSize=%d%nfailureOffsets=%s",
                                readBufferSize,
                                Arrays.deepToString(failureOffsetsCopy.toArray())));
            }

            try {
                IOUtils.copy(continuable, copied, readBufferSize);
                break;
            } catch (final IOException e) {
            }

            InputStream remaining;
            remaining = new ByteArrayInputStream(object, toIntExact(continuable.getBytesRead()), object.length);
            if (!failureOffsets.isEmpty()) {
                final Pair<Integer, Boolean> nextFailure = failureOffsets.removeFirst();
                remaining = new FailingInputStream(remaining, nextFailure.getLeft(), nextFailure.getRight());
            }

            continuable.continueWith(remaining);
        } while (!finished && copied.size() < object.length);

        assertArrayEquals("Copied object content does not match original object", object,
                copied.toByteArray());
    }

    private enum FailureOrderType {
        PRE_READ,
        POST_READ,
        PRE_READ_INITIAL_ALTERNATING,
        POST_READ_INITIAL_ALTERNATING,
    }

    private static void addToParamListIfUnseen(final Set<Integer> hashcodes,
                                               final ArrayList<Object[]> paramList,
                                               final Object[] params) {
        final int hashcode = Arrays.deepHashCode(params);
        if (hashcodes.contains(hashcode)) {
            return;
        }

        hashcodes.add(hashcode);
        paramList.add(params);
    }

    public void testAvailableCanAlsoThrow() throws IOException {
        final ContinuingInputStream cis = new ContinuingInputStream(new BrokenInputStream());

        assertThrows(IOException.class, () -> cis.available());

        assertThrows(IllegalStateException.class,  () -> cis.read());
    }

    public void testSkipCompletelySingleOperation() throws IOException {
        final ContinuingInputStream cis = new ContinuingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES));

        final long skipped = cis.skip(STUB_OBJECT_BYTES.length);
        assertEquals(skipped, STUB_OBJECT_BYTES.length);

        assertEquals(cis.read(), EOF);
        assertEquals(cis.read(new byte[1]), EOF);
        assertEquals(cis.read(new byte[1], 0, 1), EOF);

        assertEquals(cis.skip(1), 0);
    }

    public void testSkipSingleBytes() throws IOException {
        final ContinuingInputStream cis = new ContinuingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES));

        // ByteArrayInputStream provides a useful available() method
        for (int i = 0; 0 < cis.available(); i++) {
            assertEquals(cis.getBytesRead(), i);
            assertEquals(cis.skip(1), 1);
        }

        assertEquals(cis.read(), EOF);
        assertEquals(cis.read(new byte[1]), EOF);
        assertEquals(cis.read(new byte[1], 0, 1), EOF);

        assertEquals(cis.skip(1), 0);
    }

    public void testSkipThenRead() throws IOException {
        final ContinuingInputStream cis = new ContinuingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES));

        final int skipLength = 4;
        final int readLength = STUB_OBJECT_BYTES.length - skipLength;

        final long skipped = cis.skip(skipLength);
        assertEquals(skipped, cis.getBytesRead());

        final byte[] copied = new byte[readLength];

        final int bytesRead = cis.read(copied);

        assertEquals(bytesRead + skipped, cis.getBytesRead());

        final byte[] expected = ArrayUtils.subarray(STUB_OBJECT_BYTES, skipLength, skipLength + readLength);
        assertArrayEquals(expected, copied);

        assertEquals(cis.read(), EOF);
        assertEquals(cis.read(new byte[1]), EOF);
        assertEquals(cis.read(new byte[1], 0, 1), EOF);
    }

    public void testFailureFromSkipThenRead() throws IOException {
        final InputStream immediatelyPreReadFailing = new FailingInputStream(
                new ByteArrayInputStream(STUB_OBJECT_BYTES), 1, false);

        final ContinuingInputStream cis = new ContinuingInputStream(immediatelyPreReadFailing);

        assertThrows(IOException.class, () -> cis.skip(1));
        assertEquals(cis.getBytesRead(), 0);

        assertThrows(IllegalStateException.class, () -> cis.skip(1));

        cis.continueWith(new ByteArrayInputStream(STUB_OBJECT_BYTES));

        assertArrayEquals(STUB_OBJECT_BYTES, IOUtils.toByteArray(cis));
    }


}
