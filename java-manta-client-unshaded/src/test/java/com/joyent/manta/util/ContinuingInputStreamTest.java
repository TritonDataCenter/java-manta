package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.text.RandomStringGenerator;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
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

@Test
public class ContinuingInputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(ContinuingInputStreamTest.class);

    private static final int MAX_SIZE = 16;

    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .withinRange((int) 'a', (int) 'z')
                    .build();

    @AfterMethod
    public void afterMethod() {
        Mockito.validateMockitoUsage();
    }

    public void testValidatesInputs() throws Exception {
        final ContinuingInputStream mis = new ContinuingInputStream(
                new ByteArrayInputStream(RandomUtils.nextBytes(10)));
        assertThrows(NullPointerException.class, () -> mis.continueWith(null));

        // read 1 byte with space for 0 bytes
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[0], 0, 1));

        // read 0 bytes with invalid offset
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[1], -1, 0));

        // read more bytes than space available
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[1], 0, 2));

        // read more bytes than space available (due to offset)
        assertThrows(IndexOutOfBoundsException.class, () -> mis.read(new byte[2], 1, 2));

        final ContinuingInputStream closed = new ContinuingInputStream(new NullInputStream(1));
        closed.close();

        assertThrows(IllegalStateException.class, () -> closed.continueWith(new NullInputStream(0)));

        assertThrows(IllegalStateException.class, () -> closed.read());

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1]));
    }

    public void testMiscInputStreamMethods() throws Exception {
        final ContinuingInputStream mis = new ContinuingInputStream(new NullInputStream(1));

        assertFalse(mis.markSupported());
    }

    public void testCloseClosesWrappedInputStream() throws Exception {
        final InputStream source = mock(InputStream.class);
        final ContinuingInputStream mis = new ContinuingInputStream(source);
        mis.close();

        verify(source).close();
    }

    public void testClosingWithoutSettingDoesNotThrow() throws Exception {
        final ContinuingInputStream mis = new ContinuingInputStream(new NullInputStream(1));

        mis.close();
        assertTrue(true);
    }

    public void testStreamsAreClosedWhereExpected() throws Exception {
        final InputStream initial = mock(InputStream.class);
        final ContinuingInputStream mis = new ContinuingInputStream(initial);

        mis.close();
        verify(initial, times(1)).close();
        verifyNoMoreInteractions(initial);

        // extra close call doesn't throw
        mis.close();
    }

    private static final byte[] STUB_OBJECT_BYTES = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',};

    public void testReadFailureBeforeCopyingBytesDoesNotAffectCount() throws Exception {

        final ContinuingInputStream mis = new ContinuingInputStream(
                new FailingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES), 1, false));
        final byte[] readBuffer = new byte[STUB_OBJECT_BYTES.length];

        // read will fail before any bytes are read
        assertThrows(IOException.class, () -> mis.read(readBuffer));

        // no bytes were read
        assertEquals(mis.getBytesRead(), 0);
    }

    public void testReadFailureAfterCopyingBytesDoesNotAffectCount() throws Exception {

        final ContinuingInputStream mis = new ContinuingInputStream(
                new FailingInputStream(new ByteArrayInputStream(STUB_OBJECT_BYTES), 1, true));
        final byte[] readBuffer = new byte[STUB_OBJECT_BYTES.length];

        // read will actually copy bytes over but fail before telling us how many
        assertThrows(IOException.class, () -> mis.read(readBuffer));

        // no bytes were (confirmed to be) read
        assertEquals(mis.getBytesRead(), 0);
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
        AssertJUnit.assertArrayEquals(expected, copied);
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

                            // if we are on the first loop, just add the params with no failures
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

                                addToParamListIfUnseen(deepHashcodes, paramLists, new Object[]{objectSize, readBufferSize, failureOffsets});
                            }
                        }
                    }
                }
            }
        }

        int tests = 0;
        for (Object[] params : paramLists) {
            tests++;
            testBytesReadUpdatesReliably((Integer) params[0], (Integer) params[1], (Deque<ImmutablePair<Integer, Boolean>>) params[2]);
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

        AssertJUnit.assertArrayEquals("Copied object content does not match original object", object, copied.toByteArray());
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

}
