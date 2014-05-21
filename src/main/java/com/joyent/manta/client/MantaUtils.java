/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.apache.commons.io.IOUtils;

/**
 * Manta utilities.
 *
 * @author Yunong Xiao
 */
public final class MantaUtils {
    private MantaUtils() {
    }

    /**
     * Read from an {@link InputStream} to a {@link String}. Closes the {@link InputStream} when done.
     *
     * @param is
     *            The {@link InputStream}
     * @return The contents of the {@link InputStream}.
     * @throws IOException
     *             If an IO exception has occured.
     */
    public static String inputStreamToString(final InputStream is) throws IOException {
        final StringWriter writer = new StringWriter();
        try {
            IOUtils.copy(is, writer);
        } finally {
            is.close();
        }
        return writer.toString();
    }

    /**
     * Reads from an {@link InputStream} and writes to a {@link File}. Closes the {@link InputStream} when done.
     *
     * @param is
     *            The {@link InputStream}
     * @param outputFile
     *            The {@link File} to write to.
     * @throws IOException
     *             If an IO exception has occured.
     */
    public static void inputStreamToFile(final InputStream is, final File outputFile) throws IOException {
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(outputFile);
            IOUtils.copy(is, fileWriter);

        } finally {
            is.close();
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }
}
