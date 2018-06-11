package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import static com.joyent.manta.util.ResumableInputStream.EOF;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.Validate.validState;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Test
public class ResumableInputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableInputStreamTest.class);

    private static final int MAX_SIZE = 16;

    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .withinRange((int) 'a', (int) 'z')
                    .build();

    /*
        These methods were originally used as data providers but the number of tests generated was getting out of hand
        (cubed alone generates 4k test when MAX_SIZE is 16. bufferSizesCubedWithIncreasingFailureFrequency generates
        more than 100k test input combinations.
     */

    @DataProvider(name = "bufferSizes")
    private static Object[][] bufferSizes() {
        final ArrayList<Object[]> paramLists = new ArrayList<>();
        for (int i = 1; i <= MAX_SIZE; i++) {
            paramLists.add(new Object[]{i});
        }
        return paramLists.toArray(new Object[][]{});
    }

    @DataProvider(name = "bufferSizesSquared")
    private static Object[][] bufferSizesSquared() {
        final ArrayList<Object[]> paramLists = new ArrayList<>();
        for (int i = 1; i <= MAX_SIZE; i++) {
            for (int j = 1; j <= MAX_SIZE; j++) {
                paramLists.add(new Object[]{i, j});
            }
        }
        return paramLists.toArray(new Object[][]{});
    }

    @DataProvider(name = "bufferSizesCubed")
    private static Object[][] bufferSizesCubed() {
        final ArrayList<Object[]> paramLists = new ArrayList<>();
        for (int i = 1; i <= MAX_SIZE; i++) {
            for (int j = 1; j <= MAX_SIZE; j++) {
                for (int k = 1; k <= MAX_SIZE; k++) {
                    paramLists.add(new Object[]{i, j, k});
                }
            }
        }
        return paramLists.toArray(new Object[][]{});
    }

    @DataProvider(name = "bufferSizesCubedWithIncreasingFailureFrequency")
    private static Object[][] bufferSizesCubedWithIncreasingFailureFrequency() {
        int totalParamLists = 0;
        final ArrayList<Object[]> paramLists = new ArrayList<>();
        final Integer[] emptyFailureOffsetInput = new Integer[0];
        final Set<Integer> deepHashcodes = new HashSet<>();
        // with MAX_SIZE set to 16 and deephashcode unique checking we can bring the total number of input combinations
        // from 352256 to 121054

        for (int i = 1; i <= MAX_SIZE; i++) {
            for (int j = 1; j <= MAX_SIZE; j++) {
                for (int k = 1; k <= MAX_SIZE; k++) {
                    // generate params for anywhere between 0 and inputSize-1 failures
                    for (int failureCount = 0; failureCount < i; failureCount++) {

                        // if we are on the first loop, just add the params with no failures
                        if (failureCount == 0) {
                            totalParamLists++;
                            addToParamListIfUnseen(deepHashcodes, paramLists, new Object[]{i, j, k, emptyFailureOffsetInput});
                            continue;
                        }

                        // for each count of failures being generated, also shift the entire set of failure offsets by an increasing amount
                        // that is up to 1 less than half of the inputSize
                        for (int failureGlobalOffset = 0; failureGlobalOffset < i; failureGlobalOffset++) {

                            final ArrayList<Integer> failureOffsets = new ArrayList<>();

                            final int divided = Math.floorDiv(i, failureCount);
                            final int failureSpacing = NumberUtils.max(1, divided);

                            for (int failure = 0; failure <= failureCount; failure++) {
                                final int failureOffset = (failure * failureSpacing) + failureGlobalOffset;
                                if (i <= failureOffset) {
                                    // the test will complain if we pass in a failure offset that is equal to the inputSize
                                    // (since the array position at input[input.length] or after that don't make sense
                                    continue;
                                }

                                failureOffsets.add(failureOffset);
                            }

                            addToParamListIfUnseen(deepHashcodes, paramLists, new Object[]{i, j, k, failureOffsets.toArray(new Integer[]{})});
                        }
                    }
                }
            }
        }

        return paramLists.toArray(new Object[][]{});
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

    public void testValidatesInputs() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new ResumableInputStream(0));
        assertThrows(IllegalArgumentException.class, () -> new ResumableInputStream(-1));

        final ResumableInputStream mis = new ResumableInputStream(1);
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

        final ResumableInputStream closed = new ResumableInputStream();
        closed.close();

        assertThrows(IllegalStateException.class, () -> closed.setSource(new NullInputStream(0)));

        assertThrows(IllegalStateException.class, () -> closed.read());

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1]));
    }

    public void testMiscInputStreamMethods() throws Exception {
        final ResumableInputStream mis = new ResumableInputStream();

        assertFalse(mis.markSupported());
    }

    public void testCloseClosesWrappedInputStream() throws Exception {
        final ResumableInputStream mis = new ResumableInputStream(1);
        final InputStream source = mock(InputStream.class);
        mis.setSource(source);
        mis.close();

        verify(source).close();
    }

    public void testClosingWithoutSettingDoesNotThrow() throws Exception {
        final ResumableInputStream mis = new ResumableInputStream();

        mis.close();
        assertTrue(true);
    }

    public void testDoubleCloseDoesNotThrow() throws Exception {
        final ResumableInputStream mis = new ResumableInputStream();
        final InputStream inner = mock(InputStream.class);
        mis.setSource(inner);

        mis.close();
        mis.close();
        assertTrue(true);
    }

    public void testStreamsAreClosedWhereExpected() throws Exception {
        final ResumableInputStream mis = new ResumableInputStream();
        final InputStream firstStream = mock(InputStream.class);
        final InputStream secondStream = mock(InputStream.class);

        mis.setSource(firstStream);
        verify(firstStream, never()).close();

        mis.setSource(secondStream);
        verify(secondStream, never()).close();

        verify(firstStream, times(1)).close();
        verifyNoMoreInteractions(firstStream);

        mis.close();
        verify(secondStream, times(1)).close();
        verifyNoMoreInteractions(secondStream);

        mis.close();
    }

    public void testReadFailureBeforeCopyingBytesDoesNotAffectCount() throws Exception {
        final byte[] bytes = RandomUtils.nextBytes(2);
        final ResumableInputStream mis = new ResumableInputStream();
        final byte[] readBuffer = new byte[bytes.length];

        // read will fail before any bytes are read
        mis.setSource(new FailingInputStream(new ByteArrayInputStream(bytes), 1, false));
        assertThrows(IOException.class, () -> mis.read(readBuffer));

        // no bytes were read
        assertEquals(mis.getCount(), 0);
    }

    public void testReadFailureAfterCopyingBytesDoesNotAffectCount() throws Exception {
        byte[] bytes;
        do {
            bytes = RandomUtils.nextBytes(2);
            // since we want to verify that the first byte was copied but the count did not change
            // we generate byte arrays until the first byte is non-zero so we can tell that the byte was actually copied
        } while (bytes[0] == 0);
        assertNotEquals(bytes[0], 0); // kinda redundant

        final ResumableInputStream mis = new ResumableInputStream();
        final byte[] readBuffer = new byte[bytes.length];

        // read will actually copy bytes over but fail before telling us how many
        mis.setSource(new FailingInputStream(new ByteArrayInputStream(bytes), 1, true));
        assertThrows(IOException.class, () -> mis.read(readBuffer));

        // no bytes were (confirmed to be) read
        assertEquals(mis.getCount(), 0);

        // TODO: is this assertion actually useful?
        // the first byte was successfully read, but we couldn't tell since the return value from read(byte[]) was lost
        // assertEquals(readBuffer[0], bytes[0]);
    }

    public void testReadSingleByteFromSingleStream() throws Exception {
        final byte[] bytes = new byte[]{1};
        final ByteArrayInputStream src = new ByteArrayInputStream(bytes);

        final ResumableInputStream mis = new ResumableInputStream(1);
        mis.setSource(src);

        final int firstByte = mis.read();
        assertEquals(firstByte, 1);

        final int expectedEOF = mis.read();
        assertEquals(expectedEOF, EOF);
    }

    public void testReadSmallSliceOfSingleLargeInput() throws Exception {
        final Object[][] paramLists = bufferSizesSquared();

        for (final Object[] untypedParams : paramLists) {
            final Integer[] params = Arrays.copyOf(untypedParams, untypedParams.length, Integer[].class);

            try {
                testReadSmallSliceOfSingleLargeInput(params[0], params[1]);
            } catch (final Exception e) {
                LOG.error("Failed testReadSmallSliceOfSingleLargeInput with inputs: {}", Arrays.deepToString(params));
                throw e;
            }
        }
        LOG.info("ResumableInputStream testReadSmallSliceOfSingleLargeInput completed all combinations without error: {}", paramLists.length);
    }

    private void testReadSmallSliceOfSingleLargeInput(final int inputSize, final int bufferSize) throws Exception {
        final byte[] bytes = STRING_GENERATOR.generate(inputSize).getBytes(UTF_8);
        final ByteArrayInputStream src = new ByteArrayInputStream(bytes);

        final ResumableInputStream mis = new ResumableInputStream(bufferSize);
        mis.setSource(src);

        final int sliceSize = Math.min(1, Math.floorDiv(bytes.length, 2));
        final byte[] multiFirstByteSlice = new byte[sliceSize];
        final int bytesRead = mis.read(multiFirstByteSlice, 0, multiFirstByteSlice.length);
        assertEquals(bytesRead, sliceSize, "Unexpected number of bytes read");

        final byte[] referenceFirstByteSlice = new byte[sliceSize];
        System.arraycopy(bytes, 0, referenceFirstByteSlice, 0, sliceSize);

        assertArrayEquals("First byte slice contents mismatch", referenceFirstByteSlice, multiFirstByteSlice);
    }

    public void testReadFirstStreamPartiallyWithAllSizeCombinations() throws Exception {
        final Object[] paramLists = bufferSizesCubed();
        for (final Object[] untypedParams : bufferSizesCubed()) {
            final Integer[] params = Arrays.copyOf(untypedParams, untypedParams.length, Integer[].class);

            try {
                testReadFirstStreamPartially(params[0], params[1], params[2]);
            } catch (final Exception e) {
                LOG.error("Failed testReadFirstStreamPartially with inputs: {}", Arrays.deepToString(params));
                throw e;
            }
        }
        LOG.info("ResumableInputStream testReadFirstStreamPartially completed all combinations without error: {}", paramLists.length);
    }

    private void testReadFirstStreamPartially(
            final int inputSize,
            final int bufferSize,
            final int copyBufferSize
    ) throws IOException {
        final byte[] bytes = STRING_GENERATOR.generate(inputSize).getBytes(UTF_8);
        final ByteArrayInputStream source = new ByteArrayInputStream(bytes);

        final ResumableInputStream mis = new ResumableInputStream(bufferSize);
        mis.setSource(source);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final long bytesCopied = IOUtils.copy(mis, baos, copyBufferSize);
        assertEquals(bytesCopied, bytes.length);
        assertArrayEquals(bytes, baos.toByteArray());

        // the source stream should be exhausted
        assertEquals(source.read(), EOF);
    }

    /**
     * This test is left as a demonstration of how someone might work directly with the ResumableInputStream,
     * later tests which dynamically slice the input into many segments or use IOUtils helpers are better real-world
     * test cases. Truthfully, I was still working up to the complexity of the next test case but it's still a nice
     * demonstration of "intermediate" complexity.
     */
    public void testCanPerformDirectReadsOnInputWithExpectedSize() throws Exception {
        final byte[] bytes = RandomUtils.nextBytes(16);
        final ResumableInputStream mis = new ResumableInputStream(4);
        final int safeBytes = 8;

        final byte[] readBuffer = new byte[bytes.length];

        mis.setSource(new FailingInputStream(new ByteArrayInputStream(bytes), safeBytes + 1, true));

        final int initialBytesRead = mis.read(readBuffer, 0, safeBytes);
        assertEquals(initialBytesRead, safeBytes);
        assertEquals(mis.getCount(), safeBytes);

        // the next read should fail
        assertThrows(IOException.class, () -> mis.read());

        // next source starts where we left off and provides remaining bytes
        mis.setSource(new ByteArrayInputStream(bytes, safeBytes, bytes.length - safeBytes));

        final int finalBytesRead = mis.read(readBuffer, safeBytes, bytes.length - safeBytes);
        assertEquals(finalBytesRead, bytes.length - safeBytes);

        // we should have read all the bytes by now
        assertEquals(mis.getCount(), bytes.length);

        // we should have all the right bytes now
        assertArrayEquals(bytes, readBuffer);

        // reading more bytes should return EOF
        assertEquals(mis.read(), EOF);

        // reading more bytes into a new buffer should return EOF and leave the buffer untouched
        final byte[] unusedBuffer = new byte[]{1};
        assertEquals(mis.read(unusedBuffer), EOF);
        assertEquals(unusedBuffer[0], 1);

        assertEquals(mis.read(unusedBuffer), EOF);
        assertEquals(mis.read(unusedBuffer, 0, unusedBuffer.length), EOF);
    }

    /**
     * This test prepares arguments for testCanRecoverFromFailureRepeatedly and injects a "read failure order".
     * True means the error will occur after the bytes were read (but before returning).
     * False means the error will occur before reading the bytes.
     * Null means the ordering is undefined and will be picked randomly for each upcoming failure.
     */
    public void testCanRecoverFromFailureWithEveryCombinationOfInputSizeInternalBufferSizeAndCopyBufferSizeWithIncreasingCountAndOffsetFailures()
            throws Exception {
        final Object[][] paramLists = bufferSizesCubedWithIncreasingFailureFrequency();
        // don't need to burden testng with the 100k combinations
        for (final Object[] params : paramLists) {
            testCanRecoverFromFailureOrderRepeatedlyWithSpecifiedFailureOrder(params, true);
            testCanRecoverFromFailureOrderRepeatedlyWithSpecifiedFailureOrder(params, false);
            testCanRecoverFromFailureOrderRepeatedlyWithSpecifiedFailureOrder(params, null);
        }
        LOG.info("ResumableInputStream testCanRecoverFromFailureWithEveryCombinationOfInputSizeInternalBufferSizeAndCopyBufferSizeWithIncreasingCountAndOffsetFailures completed all combinations without error: {}", paramLists.length * 3);
    }

    private void testCanRecoverFromFailureOrderRepeatedlyWithSpecifiedFailureOrder(final Object[] params,
                                                                                   final Boolean postReadFailure) throws Exception {
        try {
            testCanRecoverFromFailureRepeatedly(
                    (Integer) params[0],
                    (Integer) params[1],
                    (Integer) params[2],
                    (Integer[]) params[3],
                    postReadFailure);
        } catch (final Exception | AssertionError e) {
            final String readFailureDesc;
            if (postReadFailure == null) {
                readFailureDesc = "randomOrderedReadFailure";
            } else {
                readFailureDesc = postReadFailure ? "postReadFailure" : "preReadFailure";
            }

            LOG.error("Failed testCanRecoverFromFailureRepeatedly ({}) with inputs: {}", readFailureDesc, Arrays.deepToString(params));
            throw e;
        }
    }

    private void testCanRecoverFromFailureRepeatedly(
            final int inputSize,
            final int bufferSize,
            final int copyBufferSize,
            final Integer[] readFailureOffsets,
            final Boolean readFailureIsPost
    ) throws Exception {
        final byte[] bytes = RandomUtils.nextBytes(inputSize);
        // final byte[] bytes = new byte[]{'a'};

        final ResumableInputStream mis = new ResumableInputStream(bufferSize);

        final Deque<Integer> failureOffsets = new LinkedList<>(Arrays.asList(readFailureOffsets));

        final ByteArrayOutputStream copied = new ByteArrayOutputStream();

        // we exit the retry loop when 1. there are no more failures to induce, or 2. we've read the expected number of bytes
        while (!failureOffsets.isEmpty() && copied.size() < bytes.length) {
            final int nextFailure = failureOffsets.removeFirst();
            validState(-1 < nextFailure, "failure offset must be non-negative");
            validState(nextFailure < bytes.length, "failure offset must be less than input length");

            // "request" the remaining bytes, offset by how many bytes we've already successfully read
            final InputStream remainingInput = new ByteArrayInputStream(bytes, mis.getCount(), bytes.length - mis.getCount());

            // if we were passed null, randomly select a read failure ordering
            final boolean postReadFailure;
            if (readFailureIsPost == null) {
                postReadFailure = RandomUtils.nextBoolean();
            } else {
                postReadFailure = readFailureIsPost;
            }

            mis.setSource(new FailingInputStream(remainingInput, nextFailure, postReadFailure));

            try {
                IOUtils.copy(mis, copied, copyBufferSize);
            } catch (final Exception e) {
                // we don't care, we'll just try again
            }
        }

        // copy any remaining bytes
        if (copied.size() < bytes.length) {
            mis.setSource(new ByteArrayInputStream(bytes, mis.getCount(), bytes.length));
            IOUtils.copy(mis, copied, copyBufferSize);
        }

        assertEquals(copied.size(), bytes.length);
        assertArrayEquals(bytes, copied.toByteArray());
    }

    // CONSIDER: still missing a test with a mix of read() and read(byte[], int, int)
}
