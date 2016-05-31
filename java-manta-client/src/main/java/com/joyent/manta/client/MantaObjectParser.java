/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.client.util.ObjectParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * {@link ObjectParser} implementation that uses Jackson data binding. This
 * is very useful because Jackson core doesn't provide much flexibility
 * when de/serializing many different data types.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("Duplicates")
public class MantaObjectParser implements ObjectParser {
    /**
     * Jackson data binding mapper instance.
     */
    public static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper()
              .registerModule(new JavaTimeModule());
        MAPPER.getDeserializationConfig()
              .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
              .without(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        MAPPER.getSerializationConfig()
              .with(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
              .without(SerializationFeature.WRITE_NULL_MAP_VALUES);
        MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    @Override
    public <T> T parseAndClose(final InputStream in, final Charset charset,
                               final Class<T> dataClass) throws IOException {
        try {
            return MAPPER.readValue(in, dataClass);
        } finally {
            in.close();
        }
    }

    @Override
    public Object parseAndClose(final InputStream in, final Charset charset,
                                final Type dataType) throws IOException {
        try {
            @SuppressWarnings("rawtypes")
            final Class<?> clazz = Class.forName(dataType.getTypeName());
            Object parsed = MAPPER.readValue(in, clazz);

            return parsed;
        } catch (ClassNotFoundException e) {
            String msg = String.format("Unable to find class with name: %s",
                    dataType.getTypeName());
            throw new IOException(msg, e);
        } finally {
            in.close();
        }
    }

    @Override
    public <T> T parseAndClose(final Reader reader, final Class<T> dataClass)
            throws IOException {
        try {
            return MAPPER.readValue(reader, dataClass);
        } finally {
            reader.close();
        }
    }

    @Override
    public Object parseAndClose(final Reader reader, final Type dataType)
            throws IOException {
        try {
            @SuppressWarnings("rawtypes")
            final Class<?> clazz = Class.forName(dataType.getTypeName());
            return MAPPER.readValue(reader, clazz);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Unable to find class with name: %s",
                    dataType.getTypeName());
            throw new IOException(msg, e);
        } finally {
            reader.close();
        }
    }
}
