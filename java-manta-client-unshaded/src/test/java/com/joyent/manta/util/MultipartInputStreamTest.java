package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.apache.commons.lang3.RandomUtils;
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

import static com.joyent.manta.util.MultipartInputStream.EOF;
import static java.nio.charset.StandardCharsets.UTF_8;
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

        final MultipartInputStream closed = new MultipartInputStream();
        closed.close();

        assertThrows(IllegalStateException.class, () -> closed.setSource(new NullInputStream(0)));

        assertThrows(IllegalStateException.class, () -> closed.read());

        assertThrows(IllegalStateException.class, () -> closed.read(new byte[1]));
    }

    public void testMiscInputStreamMethods() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();

        assertFalse(mis.markSupported());
    }

    public void testCloseClosesWrappedInputStream() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream(1);
        final InputStream source = mock(InputStream.class);
        mis.setSource(source);
        mis.close();

        verify(source).close();
    }

    public void testReadSingleByteFromSingleStream() throws Exception {
        final byte[] bytes = new byte[]{1};
        final ByteArrayInputStream src = new ByteArrayInputStream(bytes);

        final MultipartInputStream mis = new MultipartInputStream(BUFFER_SIZE_1);
        mis.setSource(src);

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
        mis.setSource(src);

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
        mis.setSource(source);

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
        final InputStream inner = mock(InputStream.class);
        mis.setSource(inner);

        mis.close();
        mis.close();
        assertTrue(true);
    }

    public void testStreamsAreClosedWhereExpected() throws Exception {
        final MultipartInputStream mis = new MultipartInputStream();
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
        final MultipartInputStream mis = new MultipartInputStream();
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

        final MultipartInputStream mis = new MultipartInputStream();
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

    public void testReadFailureAfterCopyingHalfOfInputUpdatesCountCorrectly() throws Exception {
        // final byte[] bytes = RandomUtils.nextBytes(8);
        final byte[] bytes = new byte[] { 10, 11, 12, 13, 14, 15, 16, 17, };
        assertEquals(bytes.length, 8);
        final MultipartInputStream mis = new MultipartInputStream(2);
        final byte[] readBuffer = new byte[bytes.length];
        final int safeBytes = Math.floorDiv(bytes.length, 2);

        mis.setSource(new FailingInputStream(new ByteArrayInputStream(bytes), safeBytes + 1, true));

        final int initialBytesRead = mis.read(readBuffer, 0, safeBytes);
        assertEquals(initialBytesRead, safeBytes);
        assertEquals(mis.getCount(), safeBytes);

        // the next read should fail
        assertThrows(IOException.class, () -> {
            System.out.println("poot");
            mis.read();
        });

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

        // reading more bytes into a new buffer should leave the buffer untouched and return EOF
        final byte[] unusedBuffer = new byte[] { 1 };
        assertEquals(mis.read(unusedBuffer), EOF);
        assertEquals(unusedBuffer[0], 1);

        assertEquals(mis.read(unusedBuffer), EOF);
        assertEquals(mis.read(unusedBuffer, 0, unusedBuffer.length), EOF);
    }
}
