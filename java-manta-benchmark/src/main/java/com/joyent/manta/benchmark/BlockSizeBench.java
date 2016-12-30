package com.joyent.manta.benchmark;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Block size benchmark.
 * Source: http://stackoverflow.com/a/11394667/33611
 */
public class BlockSizeBench {
    public static void main(String... args) throws IOException {
        for (int i = 512; i <= 2 * 1024 * 1024; i *= 2)
            readWrite(i);
    }

    private static void readWrite(int blockSize) throws IOException {
        ByteBuffer bb = ByteBuffer.allocateDirect(blockSize);
        long start = System.nanoTime();
        FileChannel out = new FileOutputStream("deleteme.dat").getChannel();
        for (int i = 0; i < (1024 << 20); i += blockSize) {
            bb.clear();
            while (bb.remaining() > 0)
                if (out.write(bb) < 1) throw new AssertionError();
        }
        out.close();
        long mid = System.nanoTime();
        FileChannel in = new FileInputStream("deleteme.dat").getChannel();
        for (int i = 0; i < (1024 << 20); i += blockSize) {
            bb.clear();
            while (bb.remaining() > 0)
                if (in.read(bb) < 1) throw new AssertionError();
        }
        in.close();
        long end = System.nanoTime();
        System.out.printf("With %.1f KB block size write speed %.1f MB/s, read speed %.1f MB/s%n",
                blockSize / 1024.0, 1024 * 1e9 / (mid - start), 1024 * 1e9 / (end - mid));
    }
}
