/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Scanner;

import static java.nio.file.Files.readAllBytes;

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
        final Scanner scanner = new Scanner(is).useDelimiter("\\A");
        return scanner.hasNext() ? scanner.next() : "";
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
        Files.copy(is, outputFile.toPath());
    }

    public static String readFileToString(final File file) throws IOException {
        return new String(readAllBytes(file.toPath()));
    }

    /**
     * Checks to see if a {@link StringBuilder} ends with a given character.
     * @param builder StringBuilder to check
     * @param match character to match
     * @return true if last character in StringBuilder matches
     */
    public static boolean endsWith(StringBuilder builder, char match) {
        if (builder == null) throw new IllegalArgumentException("StringBuilder must not be null");
        if (builder.length() == 0) return false;

        final char last = builder.subSequence(builder.length() - 1, builder.length()).charAt(0);
        return last == match;
    }

    /**
     * Converts an object's toString value to a String. If an empty string, the return null;
     * @param value object to parse .toString() value from
     * @return null if toString() returns empty or if the passed in value is null, otherwise toString() value
     */
    public static String toStringEmptyToNull(Object value) {
        if (value == null) return null;

        String stringValue = value.toString();

        if (stringValue.isEmpty()) return null;

        return stringValue;
    }

    /**
     * Parses an arbitrary object for an integer. If it can't be found,
     * return null.
     * @param value Object to parse for an integer
     * @return if parsing fails, return null
     */
    public static Integer parseIntegerOrNull(Object value) {
        if (value == null) return null;

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        String string = toStringEmptyToNull(value);
        if (string == null) return null;

        Integer parsed;

        try {
            parsed = Integer.parseInt(string);
        } catch (Exception e) {
            Logger logger = LoggerFactory.getLogger(MantaUtils.class);
            String msg = "Error parsing value as integer. Value: %s";
            logger.warn(String.format(msg, value), e);
            parsed = null;
        }

        return parsed;
    }
}
