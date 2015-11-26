package com.joyent.manta.client;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final ObjectMapper MAPPER;

    static {
        MAPPER = new ObjectMapper();
    }

    @Override
    public <T> T parseAndClose(InputStream in, Charset charset,
                               Class<T> dataClass) throws IOException {
        return null;
    }

    @Override
    public Object parseAndClose(InputStream in, Charset charset,
                                Type dataType) throws IOException {
        return null;
    }

    @Override
    public <T> T parseAndClose(Reader reader, Class<T> dataClass)
            throws IOException {
        return null;
    }

    @Override
    public Object parseAndClose(Reader reader, Type dataType)
            throws IOException {
        return null;
    }
}
