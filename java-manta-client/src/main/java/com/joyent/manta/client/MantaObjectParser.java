package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.api.client.util.ObjectParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaObjectParser implements ObjectParser {
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
    public <T> T parseAndClose(InputStream in, Charset charset,
                               Class<T> dataClass) throws IOException {
        try {
            return MAPPER.readValue(in, dataClass);
        } finally {
            in.close();
        }
    }

    @Override
    public Object parseAndClose(InputStream in, Charset charset,
                                Type dataType) throws IOException {
        try {
            final Class clazz = Class.forName(dataType.getTypeName());
            return MAPPER.readValue(in, clazz);
        } catch (ClassNotFoundException e) {
            String msg = String.format("Unable to find class with name: %s",
                    dataType.getTypeName());
            throw new IOException(msg, e);
        } finally {
            in.close();
        }
    }

    @Override
    public <T> T parseAndClose(Reader reader, Class<T> dataClass)
            throws IOException {
        try {
            return MAPPER.readValue(reader, dataClass);
        } finally {
            reader.close();
        }
    }

    @Override
    public Object parseAndClose(Reader reader, Type dataType)
            throws IOException {
        try {
            final Class clazz = Class.forName(dataType.getTypeName());
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
