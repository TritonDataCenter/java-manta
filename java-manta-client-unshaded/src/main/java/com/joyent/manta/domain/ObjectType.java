/*
 * Copyright (c) 2018-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.domain;

/**
 * Enum representing the different types of objects available on Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 * @since 3.2.2
 */
public enum ObjectType {
    /**
     * A remote object that is a logical file.
     */
    FILE,
    /**
     * A remote object that is a logical directory.
     */
    DIRECTORY,
    /**
     * A remote object that is a logical bucket.
     */
    BUCKET
}
