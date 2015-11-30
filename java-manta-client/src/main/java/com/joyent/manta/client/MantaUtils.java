/**
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.util.FieldInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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

    /**
     * Parses username and subuser from an account name.
     * @param account account name to parse
     * @return an array of containing first the username, then optionally
     *         the subuser (if it exists)
     */
    public static String[] parseAccount(final String account) {
        Objects.requireNonNull(account, "Account must be present");

        final int slashPos = account.indexOf("/");

        if (account.isEmpty()) {
            throw new IllegalArgumentException("Username can't be empty");
        } else if (slashPos == -1) {
            return new String[] {account};
        } else if (slashPos == 0) {
            throw new IllegalArgumentException("Username can't begin with /");
        } else if (account.charAt(account.length() - 1) == '/') {
            throw new IllegalArgumentException("Username can't end with /");
        }

        final String username = account.substring(0, slashPos);
        final String subuser = account.substring(slashPos + 1);

        return new String[] {username, subuser};
    }

    /**
     * Serializes a specified value to a {@link String}.
     * @param value the value to be serialized
     * @return a serialized value as a {@link String}
     */
    public static String asString(final Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Enum<?>) {
            return FieldInfo.of((Enum<?>) value).getName();
        } else if (value instanceof Iterable<?>) {
            StringBuilder sb = new StringBuilder();

            Iterator<?> itr = ((Iterable<?>) value).iterator();
            while (itr.hasNext()) {
                Object next = itr.next();

                if (next != null) {
                    sb.append(next.toString());
                }

                if (itr.hasNext()) {
                    sb.append(",");
                }
            }

            return sb.toString();
        } else if (value.getClass().isArray()) {
            Object[] array = (Object[])value;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < array.length; i++) {
                Object next = array[i];

                if (next != null) {
                    sb.append(next.toString());
                }

                if (i < array.length - 1) {
                    sb.append(", ");
                }
            }

            return sb.toString();
        }

        return value.toString();
    }


    /**
     * Converts a naive CSV string to a collection.
     *
     * @param line CSV string
     * @return collection containing each value between each comma
     */
    public static Collection<String> fromCsv(final String line) {
        Objects.requireNonNull(line, "Line must be present");

        final List<String> list = new ArrayList<>();

        if (line.contains(",")) {
            String[] parts = line.split(",\\s*");

            for (String part : parts) {
                list.add(part);
            }
        } else {
            list.add(line);
        }


        return list;
    }

    /**
     * Extracts the last file or directory from the path provided.
     *
     * @param path URL or Unix-style file path
     * @return the last file or directory in path
     */
    public static String lastItemInPath(final String path) {
        Objects.requireNonNull(path, "Path must be present");

        if (path.isEmpty()) {
            throw new IllegalArgumentException("Path must not be empty");
        }

        final Path asNioPath = Paths.get(path);
        final int count = asNioPath.getNameCount();

        if (count < 1) {
            throw new IllegalArgumentException(
                    "Path doesn't have a single element to parse");
        }

        final Path lastPart = asNioPath.getName(count - 1);
        return lastPart.toString();
    }
}
