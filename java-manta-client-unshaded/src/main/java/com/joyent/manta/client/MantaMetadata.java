/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.util.NotThreadSafe;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.collections4.map.PredicatedMap;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * <p>Class for storing Manta metadata information. All metadata keys must start
 * with the string "m-". Case insensitive {@link Map} implementation that checks
 * for valid metadata key names.</p>
 *
 * <p><em>Note:</em> Manta doesn't support multiple values for HTTP header based
 * metadata. It accepts them without throwing an error, but it will only
 * ingest a single value out of multiple values.</p>
 *
 * <p>This class is NOT thread-safe.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@NotThreadSafe
public class MantaMetadata implements Map<String, String>, Cloneable, Serializable {
    /**
     * Prefix required for metadata keys being stored via HTTP headers on Manta.
     */
    public static final String METADATA_PREFIX = "m-";
    /**
     * Prefix required for encrypted metadata keys being stored in ciphertext.
     */
    public static final String ENCRYPTED_METADATA_PREFIX = "e-";
    /**
     * An array of characters considered to be illegal in metadata keys.
     */
    static final char[] ILLEGAL_KEY_CHARS = "()<>@,;:</[]?={}\\ \n\t\r".toCharArray();
    private static final long serialVersionUID = -5828336629480323042L;
    /**
     * The character value of the ASCII code for a space character (decimal value 32).
     */
    private static final char ASCIICODE_32_SPACE = ' ';
    /**
     * The backing map data structure.
     */
    private final PredicatedMap<String, String> innerMap;

    /**
     * Create a new instance backed with the specified map.
     * @param m the backing map
     */
    public MantaMetadata(final Map<? extends String, ? extends String> m) {
        this();
        putAll(m);
    }

    /**
     * Create a new instance backed with a new empty map.
     */
    public MantaMetadata() {
        final Map<String, String> map = new CaseInsensitiveMap<>();
        final Predicate<String> keyPredicate = new HttpHeaderNameKeyPredicate();
        this.innerMap = PredicatedMap.predicatedMap(map, keyPredicate, null);
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected Object clone() throws CloneNotSupportedException {
        return new MantaMetadata(this);
    }

    /**
     * Removes all metadata with keys prefixed by <code>e-</code>.
     */
    public void removeAllEncrypted() {
        final Set<Map.Entry<String, String>> set = entrySet();

        set.removeIf(entry -> entry.getKey()
           .startsWith(ENCRYPTED_METADATA_PREFIX));
    }

    /**
     * Deletes user-supplied metadata associated with a Manta object.
     * @param key key to delete
     */
    public void delete(final String key) {
        put(key, null);
    }

    @Override
    public String merge(final String key,
                        final String value,
                        final BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return innerMap.merge(key, value, remappingFunction);
    }

    @Override
    public String compute(final String key,
                          final BiFunction<? super String, ? super String, ? extends String> remappingFunction) {
        return innerMap.compute(key, remappingFunction);
    }

    @Override
    public String computeIfPresent(final String key,
                                   final BiFunction<
                                           ? super String,
                                           ? super String,
                                           ? extends String
                                           > remappingFunction) {
        return innerMap.computeIfPresent(key, remappingFunction);
    }

    @Override
    public String computeIfAbsent(final String key, final Function<? super String, ? extends String> mappingFunction) {
        return innerMap.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public String replace(final String key, final String value) {
        return innerMap.replace(key, value);
    }

    @Override
    public boolean replace(final String key, final String oldValue, final String newValue) {
        return innerMap.replace(key, oldValue, newValue);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return innerMap.remove(key, value);
    }

    @Override
    public String putIfAbsent(final String key, final String value) {
        return innerMap.putIfAbsent(key, value);
    }

    @Override
    public void replaceAll(final BiFunction<? super String, ? super String, ? extends String> function) {
        innerMap.replaceAll(function);
    }

    @Override
    public void forEach(final BiConsumer<? super String, ? super String> action) {
        innerMap.forEach(action);
    }

    @Override
    public String getOrDefault(final Object key, final String defaultValue) {
        return innerMap.getOrDefault(key, defaultValue);
    }

    @Override
    public int hashCode() {
        return innerMap.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return getClass().equals(object.getClass())
                && innerMap.equals(object);
    }

    @Override
    public Collection<String> values() {
        return innerMap.values();
    }

    @Override
    public int size() {
        return innerMap.size();
    }

    @Override
    public String remove(final Object key) {
        return innerMap.remove(key);
    }

    @Override
    public Set<String> keySet() {
        return innerMap.keySet();
    }

    @Override
    public boolean isEmpty() {
        return innerMap.isEmpty();
    }

    @Override
    public String get(final Object key) {
        return innerMap.get(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return innerMap.containsValue(value);
    }

    @Override
    public boolean containsKey(final Object key) {
        return innerMap.containsKey(key);
    }

    @Override
    public void clear() {
        innerMap.clear();
    }

    @Override
    public String put(final String key, final String value) {
        return innerMap.put(key, value);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends String> mapToCopy) {
        innerMap.putAll(mapToCopy);
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return innerMap.entrySet();
    }

    @Override
    public String toString() {
        String baseInfo = new ToStringBuilder(this)
                .append("innerMap", innerMap).toString();
        StringBuilder builder = new StringBuilder(baseInfo).append("\n");

        for (Map.Entry<String, String> entry : innerMap.entrySet()) {
            builder.append(" [").append(entry.getKey()).append("] = [")
                   .append(entry.getValue()).append("]\n");
        }

        return builder.toString();
    }

    /**
     * Implements the predicate used to validate header key values.
     */
    protected static class HttpHeaderNameKeyPredicate implements Predicate<String> {

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean evaluate(final String object) {
            return object != null
                    && !object.isEmpty()
                    && !hasIllegalChars(object)
                    && isIso88591(object)
                    && validPrefix(object);
        }

        /**
         * Test a string for iso8859-1 character encoding.
         *
         * @param input string value to be tested
         * @return true if the string is entirely iso8859-1, false otherwise.
         */
        private boolean isIso88591(final String input) {
            try {
                final byte[] bytes = input.getBytes("ISO-8859-1");
                final String result = new String(bytes, "ISO-8859-1");
                return result.equals(input);
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("JVM doesn't support \"ISO-8859-1\" encoding", e);
            }
        }

        /**
         * Test a string starts with a valid prefix.
         *
         * @param input string value to be tested
         * @return true if the string starts with a valid prefix, false otherwise.
         */
        private boolean validPrefix(final String input) {
            return input.toLowerCase(Locale.ENGLISH).startsWith(METADATA_PREFIX)
                   || input.toLowerCase(Locale.ENGLISH).startsWith(ENCRYPTED_METADATA_PREFIX);
        }

        /**
         * Test a string for illegal characters.
         *
         * @param input string value to be tested
         * @return true if the string contains illegal characters, false otherwise.
         */
        private boolean hasIllegalChars(final String input) {
            final char[] chars = input.toCharArray();

            for (final char c : chars) {
                if (isControlCharacter(c)) {
                    return true;
                }

                for (char illegalKeyChar : ILLEGAL_KEY_CHARS) {
                    if (c == illegalKeyChar) {
                        return true;
                    }
                }
            }

            return false;
        }

        /**
         * Test if a character is considered a control characters.
         *
         * @param c character value to be tested
         * @return true if the character is a control character, false otherwise.
         */
        private boolean isControlCharacter(final char c) {
            final int intVal = (int)c;
            return intVal < ASCIICODE_32_SPACE;
        }

    }
}
