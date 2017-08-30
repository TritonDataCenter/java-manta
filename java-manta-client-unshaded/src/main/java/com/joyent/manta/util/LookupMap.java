/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.collections4.map.UnmodifiableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Custom built {@link Map} implementation that supports case-sensitive and
 * case-insensitive operations.
 *
 * @param <String> key as string because we provide case-insensitive operations
 * @param <V> value to be looking up
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class LookupMap<String, V> implements Map<String, V>  {
    /**
     /**
     * Wrapped unmodifiable map instance providing data.
     */
    private final Map<String, V> wrapped;

    /**
     * Wrapped unmodifiable case-insensitive instance providing data.
     */
    private final CaseInsensitiveMap<String, V> lowercaseWrapped;

    /**
     * Creates a new instance of a lookup map backed by the specified map.
     *
     * @param backingMap map to back lookup map
     */
    public LookupMap(final Map<String, V> backingMap) {
        if (isUnmodifiable(backingMap)) {
            this.wrapped = backingMap;
        } else {
            this.wrapped = Collections.unmodifiableMap(backingMap);
        }
        this.lowercaseWrapped = new CaseInsensitiveMap<>(this.wrapped);
    }

    /**
     * Determines if a given map is of a unmodifiable type.
     *
     * @param map map to check
     * @return true if unmodifiable
     */
    private boolean isUnmodifiable(final Map<String, V> map) {
        return map.getClass().getName().equals("java.util.Collections$UnmodifiableMap")
               || map.getClass().getName().equals("org.apache.commons.collections4.map.UnmodifiableMap")
               || map.getClass().getName().equals("com.google.common.collect.ImmutableMap")
               || map instanceof UnmodifiableMap;
    }

    /**
     * @return the key set all in lowercase.
     */
    public Set<String> lowercaseKeySet() {
        return this.lowercaseWrapped.keySet();
    }

    /**
     * Searches the map and determines if a case-insensitive key exists.
     * @param key case-insensitive key
     * @return true if exists otherwise false
     */
    public boolean containsKeyCaseInsensitive(final String key) {
        return this.lowercaseWrapped.containsKey(key);
    }

    /**
     * Searches the map for a value by a case-insensitive key.
     * @param key case-insensitive key
     * @return associated value or null when not found
     */
    public V getWithCaseInsensitiveKey(final String key) {
        return this.lowercaseWrapped.get(key);
    }

    @Override
    public int size() {
        return wrapped.size();
    }

    @Override
    public boolean isEmpty() {
        return wrapped.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return wrapped.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return wrapped.containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return wrapped.get(key);
    }

    @Override
    public V put(final String key, final V value) {
        return wrapped.put(key, value);
    }

    @Override
    public V remove(final Object key) {
        return wrapped.remove(key);
    }

    @Override
    public void putAll(final Map<? extends String, ? extends V> m) {
        wrapped.putAll(m);
    }

    @Override
    public void clear() {
        wrapped.clear();
    }

    @Override
    public Set<String> keySet() {
        return wrapped.keySet();
    }

    @Override
    public Collection<V> values() {
        return wrapped.values();
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return wrapped.entrySet();
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object o) {
        return wrapped.equals(o);
    }

    @Override
    public int hashCode() {
        return wrapped.hashCode();
    }

    @Override
    public V getOrDefault(final Object key, final V defaultValue) {
        return wrapped.getOrDefault(key, defaultValue);
    }

    @Override
    public void forEach(final BiConsumer<? super String, ? super V> action) {
        wrapped.forEach(action);
    }

    @Override
    public void replaceAll(final BiFunction<? super String, ? super V, ? extends V> function) {
        wrapped.replaceAll(function);
    }

    @Override
    public V putIfAbsent(final String key, final V value) {
        return wrapped.putIfAbsent(key, value);
    }

    @Override
    public boolean remove(final Object key, final Object value) {
        return wrapped.remove(key, value);
    }

    @Override
    public boolean replace(final String key, final V oldValue, final V newValue) {
        return wrapped.replace(key, oldValue, newValue);
    }

    @Override
    public V replace(final String key, final V value) {
        return wrapped.replace(key, value);
    }

    @Override
    public V computeIfAbsent(final String key, final Function<? super String, ? extends V> mappingFunction) {
        return wrapped.computeIfAbsent(key, mappingFunction);
    }

    @Override
    public V computeIfPresent(final String key,
                              final BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        return wrapped.computeIfPresent(key, remappingFunction);
    }

    @Override
    public V compute(final String key,
                     final BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
        return wrapped.compute(key, remappingFunction);
    }

    @Override
    public V merge(final String key,
                   final V value,
                   final BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
        return wrapped.merge(key, value, remappingFunction);
    }
}
