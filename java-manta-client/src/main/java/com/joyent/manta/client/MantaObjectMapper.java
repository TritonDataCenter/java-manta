/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Provider class of {@link ObjectMapper} that is configured for use with
 * the Manta SDK.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaObjectMapper extends ObjectMapper {
    private static final long serialVersionUID = -54543439989941209L;

    /**
     * Jackson data binding mapper instance.
     */
    public static final ObjectMapper INSTANCE = new MantaObjectMapper();

    /**
     * Creates a configured instance of {@link ObjectMapper}.
     */
    public MantaObjectMapper() {
        registerModule(new JavaTimeModule());
        getDeserializationConfig()
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .without(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        getSerializationConfig()
                .with(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
                .without(SerializationFeature.WRITE_NULL_MAP_VALUES);
        setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
