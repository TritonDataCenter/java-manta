/*
 * Copyright (c) 2013, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ContextedException;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.http.client.utils.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.URLEncoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Manta utilities.
 *
 * @author Yunong Xiao
 */
public final class MantaUtils {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(MantaUtils.class);

    /**
     * Default no-args constructor.
     */
    private MantaUtils() {
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
            String msg = "Error parsing value as integer. Value: %s";
            LOGGER.warn(String.format(msg, value), e);
            parsed = null;
        }

        return parsed;
    }


    /**
     * Parses an arbitrary object for a boolean. If it can't be found, return null.
     *
     * @param value Object to parse for an boolean
     * @return if parsing fails, return null
     */
    public static Boolean parseBooleanOrNull(final Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean)value;
        }

        String string = toStringEmptyToNull(value);
        if (string == null) {
            return null;
        }

        if (string.equals("1")) {
            return true;
        } else if (string.equals("0")) {
            return false;
        }

        Boolean parsed;

        try {
            parsed = BooleanUtils.toBoolean(string);
        } catch (Exception e) {
            String msg = "Error parsing value as boolean. Value: %s";
            LOGGER.warn(String.format(msg, value), e);
            parsed = null;
        }

        return parsed;
    }


    /**
     * Parses an arbitrary object for an enum represented as a String. If the
     * string is empty or null we return null. If the String doesn't match any
     * valid enum values, then we return null
     *
     * @param value object to parse string value from
     * @param enumClass enum class to parse
     * @param <T> enum type to parse
     * @return value of enum or null on invalid input
     */
    public static <T extends Enum<T>> T parseEnumOrNull(final Object value,
                                                        final Class<T> enumClass) {
        if (value == null) {
            return null;
        }

        if (value.getClass() == enumClass) {
            @SuppressWarnings("unchecked")
            T cast = (T)value;
            return cast;
        }

        String string = toStringEmptyToNull(value);
        if (string == null) {
            return null;
        }

        T parsed;

        try {
            parsed = Enum.valueOf(enumClass, string);
        } catch (RuntimeException e) {
            String msg = "Error parsing value as enum. Value: %s";
            LOGGER.warn(String.format(msg, value), e);
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
            Enum<?> enumValue = (Enum<?>)value;

            try {
                Field field = enumValue.getClass().getField(enumValue.name());
                Validate.notNull(field,
                        "A non-null field should always be returned. "
                            + "Enum constant missing @Value or @NullValue annotation: %s",
                        enumValue);
            } catch (NoSuchFieldException e) {
                String msg = String.format("Could not find name field for enum: %s",
                        value);
                LOGGER.warn(msg, e);
                return null;
            }
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
            Collections.addAll(list, parts);
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

    /**
     * Converts a map of string to object values to a pure string map.
     *
     * @param map map to convert
     * @return a string map
     */
    public static Map<String, String> asStringMap(final Map<String, ?> map) {
        Objects.requireNonNull(map, "Map must be present");

        if (map.isEmpty()) {
            return Collections.emptyMap();
        }

        final Map<String, String> stringMap = new LinkedHashMap<>(map.size());

        // Silly Java generics won't covert wildcard to simple generic
        @SuppressWarnings("unchecked")
        final Map<String, Object> objectMap = (Map<String, Object>)map;
        final Set<Map.Entry<String, Object>> entrySet = objectMap.entrySet();

        for (Map.Entry<String, Object> next : entrySet) {
            final Object obj = next.getValue();
            final String value;

            if (obj == null || obj instanceof String) {
                value = (String) obj;
            } else if (obj instanceof InetAddress) {
                value = ((InetAddress) obj).getHostAddress();
            } else if (obj instanceof Map) {
                value = csv((Map) obj);
            } else if (obj instanceof Iterable) {
                value = csv((Iterable) obj);
            } else if (obj instanceof CharSequence) {
                value = String.valueOf(obj);
            } else {
                value = obj.toString();
            }

            stringMap.put(next.getKey(), value);
        }

        return Collections.unmodifiableMap(stringMap);
    }

    /**
     * Naively converts a map to a single CSV string. Warning: this doesn't
     * escape.
     *
     * @param map map with objects with implemented toString methods
     * @return CSV string or empty string
     */
    public static String csv(final Map<?, ?> map) {
        Objects.requireNonNull(map, "Map must be present");

        final StringBuilder builder = new StringBuilder();

        /* We do this contorted type conversion because of Java's generics. */
        @SuppressWarnings("rawtypes")
        final Map noGenericsMap = map;
        @SuppressWarnings({"rawtypes", "unchecked"})
        final Set<Map.Entry<?, ?>> set = noGenericsMap.entrySet();

        final Iterator<Map.Entry<?, ?>> itr = set.iterator();

        while (itr.hasNext()) {
            Map.Entry<?, ?> entry = itr.next();

            if (entry == null || entry.getKey() == null) {
                continue;
            }

            builder.append(entry.getKey().toString())
                    .append(": ")
                    .append(String.valueOf(entry.getValue()));

            if (itr.hasNext()) {
                builder.append(", ");
            }
        }

        return builder.toString();
    }

    /**
     * Naively converts a collection of objects to a single CSV string.
     * Warning: this doesn't escape.
     * @param stringable collection of objects with implemented toString methods
     * @return CSV string or empty string
     */
    public static String csv(final Iterable<?> stringable) {
        if (stringable == null) {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        Iterator<?> itr = stringable.iterator();

        while (itr.hasNext()) {
            final Object o = itr.next();

            if (o == null) {
                continue;
            }

            final String value;

            if (o instanceof InetAddress) {
                value = ((InetAddress)o).getHostAddress();
            } else if (o instanceof Map) {
                StringBuilder sb = new StringBuilder();

                @SuppressWarnings({ "unchecked", "rawtypes" })
                final Map map = (Map)o;
                @SuppressWarnings("unchecked")
                final Iterator<Map.Entry<?, ?>> mapItr =
                        (Iterator<Map.Entry<?, ?>>)map.entrySet().iterator();

                while (mapItr.hasNext()) {
                    Map.Entry<?, ?> entry = mapItr.next();
                    sb.append("[")
                            .append(String.valueOf(entry.getKey()))
                            .append("=")
                            .append(entry.getValue())
                            .append("]");

                    if (mapItr.hasNext()) {
                        sb.append(" ");
                    }
                }

                value = sb.toString();
            } else {
                value = o.toString();
            }

            // Strip any commas out of the string
            builder.append(StringUtils.replaceChars(value, ',', ' '));

            if (itr.hasNext()) {
                builder.append(", ");
            }
        }

        return builder.toString();
    }

    /**
     * Naively converts a CSV string into an array.
     *
     * @param line non-null String containing comma delimiters
     * @return an array of Strings for each token between a comma
     */
    public static String[] csv2array(final String line) {
        Objects.requireNonNull(line, "Line must be present");

        if (line.contains(",")) {
            return line.split(",\\s*");
        } else {
            return new String[] {line};
        }
    }

    /**
     * Adds the passed exceptions as properties to a {@link ContextedException}
     * instance.
     *
     * @param contexted exception to attach exceptions to
     * @param exceptions exceptions to attach
     */
    public static void attachExceptionsToContext(final ExceptionContext contexted,
                                                 final Iterable<? extends Exception> exceptions) {
        int count = 1;
        for (Exception e : exceptions) {
            final String label = String.format("exception_%d", count++);
            contexted.setContextValue(label, e);
        }
    }

    /**
     * Parses a HTTP Date header value.
     *
     * @param date header to parse
     * @return instance of Date based on input
     */
    public static Date parseHttpDate(final String date) {
        return DateUtils.parseDate(date);
    }
}
