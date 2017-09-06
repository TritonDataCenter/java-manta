package com.joyent.manta.util;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

public class InfiniteInputStreamTest {

    @Test
    public void testReadWithoutLoop() throws Exception {

        final byte[] checkBuffer = new byte[10];

        final InfiniteInputStream infiniteInput = new InfiniteInputStream(StringUtils.repeat("abc", 5).getBytes(StandardCharsets.UTF_8));

        IOUtils.read(infiniteInput, checkBuffer);

        AssertJUnit.assertArrayEquals("abcabcabca".getBytes(StandardCharsets.UTF_8), checkBuffer);
    }

    @Test
    public void testReadSingleByteIndefinitely() throws Exception {

        final byte[] checkBuffer = new byte[10];

        final InfiniteInputStream infiniteInput = new InfiniteInputStream(new byte[]{'a'});

        IOUtils.readFully(infiniteInput, checkBuffer, 0, 5);

        AssertJUnit.assertArrayEquals("aaaaa\0\0\0\0\0".getBytes(StandardCharsets.UTF_8), checkBuffer);
    }

}