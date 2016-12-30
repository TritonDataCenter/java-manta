/*
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.http.entity;

import java.nio.ByteBuffer;

/**
 * Provides an interface for entity that can expose a backing buffer of their
 * content.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public interface MemoryBackedEntity {
    /**
     * @return the backing byte array as a {@link ByteBuffer} instance
     */
    ByteBuffer getBackingBuffer();
}
