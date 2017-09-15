/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
     * JSON node factory instance.
     */
    public static final JsonNodeFactory NODE_FACTORY_INSTANCE =
            new JsonNodeFactory(false);

    /**
     * Creates a configured instance of {@link ObjectMapper}.
     */
    public MantaObjectMapper() {
        registerModule(new JavaTimeModule());

        DeserializationConfig deserializationConfig = getDeserializationConfig()
                .without(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .without(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES);
        setConfig(deserializationConfig);

        SerializationConfig serializationConfig = getSerializationConfig()
                .with(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS)
                .without(SerializationFeature.WRITE_NULL_MAP_VALUES);
        setConfig(serializationConfig);

        setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }
}
