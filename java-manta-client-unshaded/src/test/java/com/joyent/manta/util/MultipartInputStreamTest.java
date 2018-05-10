package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.text.RandomStringGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import static com.joyent.manta.util.MultipartInputStream.EOF;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Test
public class MultipartInputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(MultipartInputStreamTest.class);

    private static final int BUFFER_SIZE_1 = 1;
    private static final int BUFFER_SIZE_256 = 1 << 8;
    private static final int BUFFER_SIZE_1K = 1 << 10;
    private static final int BUFFER_SIZE_8K = 1 << 13;

    private static final int[] BUFFER_SIZES = {
            BUFFER_SIZE_1,
            BUFFER_SIZE_256 - 1,
            BUFFER_SIZE_256,
            BUFFER_SIZE_256 + 1,
            BUFFER_SIZE_1K - 1,
            BUFFER_SIZE_1K,
            BUFFER_SIZE_1K + 1,
            BUFFER_SIZE_8K - 1,
            BUFFER_SIZE_8K,
            BUFFER_SIZE_8K + 1,
    };

    private static final RandomStringGenerator STRING_GENERATOR =
            new RandomStringGenerator.Builder()
                    .withinRange((int) 'a', (int) 'z')
                    .build();

    @DataProvider(name = "bufferSizes")
    private Object[][] bufferSizes() {
        final Object[][] parameterLists = new Object[BUFFER_SIZES.length][1];
        for (int i = 0; i < parameterLists.length; i++) {
            parameterLists[i][0] = BUFFER_SIZES[i];
        }
        return parameterLists;
    }

    @DataProvider(name = "cartesianProductOfSizes")
    private Object[][] cartesianProductOfSizes() {
        final Object[][] parameterLists = new Object[BUFFER_SIZES.length * BUFFER_SIZES.length][2];
        for (int i = 0; i < parameterLists.length; i++) {
            parameterLists[i][0] = BUFFER_SIZES[i / BUFFER_SIZES.length];
            parameterLists[i][1] = BUFFER_SIZES[i % BUFFER_SIZES.length];
        }

        return parameterLists;
    }

    @DataProvider(name = "inputAndBufferAndCopyBufferSizes")
    private Object[][] inputAndBufferAndCopyBufferSizes() {
        final ArrayList<Object[]> parameterLists = new ArrayList<>();

        final float[] mult = new float[]{0.5f, 1f, 2f};
        for (int i = 0; i < BUFFER_SIZES.length; i++) {
            for (int j = 0; j < BUFFER_SIZES.length; j++) {
                for (int k = 0; k < mult.length; k++) {
                    parameterLists.add(new Object[]{BUFFER_SIZES[i], BUFFER_SIZES[j], Math.max(1, (int) (BUFFER_SIZES[j] * mult[k]))});
                }
            }
        }

        final Object[][] params = parameterLists.toArray(new Object[][]{});
        return params;
    }

    public void testValidatesInputs() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> new MultipartInputStream(0));
        assertThrows(IllegalArgumentException.class, () -> new MultipartInputStream(-1));

        final MultipartInputStream mis = new MultipartInputStream(1);
        assertThrows(NullPointerException.class, () -> mis.setNext(null));

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
    }

    public void testMiscInputStreamMethods() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();

        assertFalse(mis.markSupported());
    }

    public void testCloseClosesWrappedInputStream() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream(1);
        final InputStream source = mock(InputStream.class);
        mis.setNext(source);
        mis.close();

        verify(source).close();
    }

    public void testReadSingleByteFromSingleStream() throws Exception {
        final byte[] bytes = new byte[]{1};
        final ByteArrayInputStream src = new ByteArrayInputStream(bytes);

        final MultipartInputStream mis = new MultipartInputStream(BUFFER_SIZE_1);
        mis.setNext(src);

        final int firstByte = mis.read();
        assertEquals(firstByte, 1);

        final int expectedEOF = mis.read();
        assertEquals(expectedEOF, EOF);
    }

    @Test(dataProvider = "cartesianProductOfSizes")
    public void testReadSmallSliceOfSingleLargeInput(final int inputSize, final int bufferSize) throws Exception {
        final byte[] bytes = STRING_GENERATOR.generate(inputSize).getBytes(UTF_8);
        final ByteArrayInputStream src = new ByteArrayInputStream(bytes);

        final MultipartInputStream mis = new MultipartInputStream(bufferSize);
        mis.setNext(src);

        final int sliceSize = Math.min(1, Math.floorDiv(bytes.length, 2));
        final byte[] multiFirstByteSlice = new byte[sliceSize];
        final int bytesRead = mis.read(multiFirstByteSlice, 0, multiFirstByteSlice.length);
        assertEquals(bytesRead, sliceSize, "Unexpected number of bytes read");

        final byte[] referenceFirstByteSlice = new byte[sliceSize];
        System.arraycopy(bytes, 0, referenceFirstByteSlice, 0, sliceSize);

        assertArrayEquals("First byte slice contents mismatch", referenceFirstByteSlice, multiFirstByteSlice);
    }

    @Test(dataProvider = "inputAndBufferAndCopyBufferSizes")
    public void testReadFirstStreamPartially(
            final int inputSize,
            final int bufferSize,
            final int copyBufferSize
    ) throws IOException {
        final byte[] bytes = STRING_GENERATOR.generate(inputSize).getBytes(UTF_8);
        final ByteArrayInputStream source = new ByteArrayInputStream(bytes);

        final MultipartInputStream mis = new MultipartInputStream(bufferSize);
        mis.setNext(source);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        final long bytesCopied = IOUtils.copy(mis, baos, copyBufferSize);
        assertEquals(bytesCopied, bytes.length);
        assertArrayEquals(bytes, baos.toByteArray());

        // the source stream should be exhausted
        assertEquals(source.read(), EOF);
    }

    // TODO: still missing a test where the input is larger than 4096 (the default buffer size of IOUtils.copy
    // TODO: still missing a test with a mix of read() and read(byte[], int, int)

    public void testClosingWithoutSettingDoesNotThrow() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();

        mis.close();
        assertTrue(true);
    }

    public void testDoubleCloseDoesNotThrow() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();
        mis.setNext(mock(InputStream.class));

        mis.close();
        mis.close();
        assertTrue(true);
    }

    public void testStreamsAreClosedWhereExpected() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();
        final InputStream firstStream = mock(InputStream.class);
        final InputStream secondStream = mock(InputStream.class);

        mis.setNext(firstStream);
        verify(firstStream, never()).close();

        mis.setNext(secondStream);
        verify(secondStream, never()).close();

        verify(firstStream, times(1)).close();
        verifyNoMoreInteractions(firstStream);

        mis.close();
        verify(secondStream, times(1)).close();
        verifyNoMoreInteractions(secondStream);

        mis.close();
    }

    @Test(dataProvider = "bufferSizes")
    public void testSingleObjectCanBeReadAcrossTwoStreams(final int inputSize) throws Exception {
        final byte[] bytes = STRING_GENERATOR.generate(inputSize).getBytes(US_ASCII);

        final int firstSegmentSize = Math.floorDiv(bytes.length, 2);
        final int lastSegmentSize = bytes.length - firstSegmentSize;

        final MultipartInputStream mis = new MultipartInputStream();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);

        // the first stream provides bytes `0` through `firstSegmentSize - 1`
        mis.setNext(new FailingInputStream(new ByteArrayInputStream(bytes), firstSegmentSize));

        assertThrows(IOException.class, () -> IOUtils.copy(mis, baos));
        assertEquals(mis.getCount(), firstSegmentSize);

        // the second stream provides bytes `firstSegmentSize - 1` through `bytes.length`
        mis.setNext(new ByteArrayInputStream(bytes, firstSegmentSize, bytes.length - firstSegmentSize));

        IOUtils.copy(mis, baos);

        AssertJUnit.assertArrayEquals(baos.toByteArray(), bytes);
    }
}
