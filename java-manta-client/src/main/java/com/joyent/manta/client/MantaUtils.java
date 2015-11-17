/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Objects;
import java.util.Scanner;

import static java.nio.file.Files.readAllBytes;

/**
 * Manta utilities.
 *
 * @author Yunong Xiao
 */
public final class MantaUtils {


    /**
     * Default no-args constructor.
     */
    private MantaUtils() {
    }


    /**
     * Read from an {@link java.io.InputStream} to a {@link java.lang.String}.
     * Closes the {@link java.io.InputStream} when done.
     *
     * @param inputStream The {@link java.io.InputStream}
     * @param charsetName The encoding type used to convert bytes from the
     *        stream into characters to be scanned
     * @return The contents of the {@link java.io.InputStream}
     * @throws IOException If an IO exception has occurred
     */
    public static String inputStreamToString(final InputStream inputStream,
                                             final String charsetName) throws IOException {
        final Scanner scanner = new Scanner(inputStream, charsetName)
                .useDelimiter("\\A");
        String nextToken = "";
        if (scanner.hasNext()) {
            nextToken = scanner.next();
        }
        return nextToken;
    }


    /**
     * Read from an {@link java.io.InputStream} to a {@link java.lang.String}
     * using the default encoding. Closes the {@link java.io.InputStream} when done.
     *
     * @param inputStream The {@link java.io.InputStream}
     * @return The contents of the {@link java.io.InputStream}
     * @throws IOException If an IO exception has occurred
     */
    public static String inputStreamToString(final InputStream inputStream) throws IOException {
        Objects.requireNonNull(inputStream, "InputStream should be present");
        return inputStreamToString(inputStream, Charset.defaultCharset().name());
    }


    /**
     * Reads from an {@link java.io.InputStream} and writes to a {@link java.io.File}.
     * Closes the {@link java.io.InputStream} when done.
     *
     * @param inputStream The {@link java.io.InputStream}
     * @param outputFile The {@link java.io.File} to write to
     * @throws IOException If an IO exception has occurred
     */
    public static void inputStreamToFile(final InputStream inputStream, final File outputFile) throws IOException {
        Files.copy(
                inputStream,
                outputFile.toPath()
        );
    }


    /**
     * Reads the contents of the specified {@link java.io.File} into a {@link java.lang.String}.
     *
     * @param file the {@link java.io.File} to read from
     * @return a {@link java.lang.String} containing the contents of the specified {@link java.io.File}
     * @throws IOException if an IO exception has occurred
     */
    public static String readFileToString(final File file) throws IOException {
        return new String(
                readAllBytes(
                        file.toPath()
                )
        );
    }


    /**
     * Checks to see if a {@link StringBuilder} ends with a given character.
     *
     * @param builder StringBuilder to check
     * @param match character to match
     * @return true if last character in StringBuilder matches
     */
    public static boolean endsWith(final StringBuilder builder, final char match) {
        if (builder == null) {
            throw new IllegalArgumentException("StringBuilder must not be null");
        }
        if (builder.length() == 0) {
            return false;
        }

        final char last = builder.subSequence(builder.length() - 1, builder.length()).charAt(0);
        return last == match;
    }


    /**
     * Converts an object's toString value to a String. If an empty string, the return null.
     *
     * @param value object to parse .toString() value from
     * @return null if toString() returns empty or if the passed in value is null, otherwise toString() value
     */
    public static String toStringEmptyToNull(final Object value) {
        if (value == null) {
            return null;
        }

        String stringValue = value.toString();

        if (stringValue.isEmpty()) {
            return null;
        }

        return stringValue;
    }

    /**
     * Parses an arbitrary object for an integer. If it can't be found, return null.
     *
     * @param value Object to parse for an integer
     * @return if parsing fails, return null
     */
    public static Integer parseIntegerOrNull(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        String string = toStringEmptyToNull(value);
        if (string == null) {
            return null;
        }

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


    /**
     * Format the path according to RFC3986.
     *
     * @param path the raw path string.
     * @return the URI formatted string with the exception of '/' which is special in manta.
     * @throws UnsupportedEncodingException If UTF-8 is not supported on this system.
     */
    public static String formatPath(final String path) throws UnsupportedEncodingException {
        // first split the path by slashes.
        final String[] elements = path.split("/");
        final StringBuilder encodedPath = new StringBuilder();
        for (final String string : elements) {
            if (string.equals("")) {
                continue;
            }
            encodedPath.append("/").append(URLEncoder.encode(string, "UTF-8"));
        }
        return encodedPath.toString();
    }
}
