package com.joyent.manta.util;

import com.joyent.manta.http.InputStreamContinuator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FileInputStreamContinuator implements InputStreamContinuator {

    private static final Logger LOG = LoggerFactory.getLogger(FileInputStreamContinuator.class);

    private final File file;

    public FileInputStreamContinuator(final File file) {
        this.file = file;
    }

    @Override
    public InputStream buildContinuation(final IOException ex, final long bytesRead) throws IOException {

        final FileInputStream fileStream = new FileInputStream(this.file);

        long skipRemaining = bytesRead;
        do {
            skipRemaining -= fileStream.skip(bytesRead);
        } while (0 < skipRemaining);

        return fileStream;
    }

    @Override
    public void close() throws IOException {
    }
}
