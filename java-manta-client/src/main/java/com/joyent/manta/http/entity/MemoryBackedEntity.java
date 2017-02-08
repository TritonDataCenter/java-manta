/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
