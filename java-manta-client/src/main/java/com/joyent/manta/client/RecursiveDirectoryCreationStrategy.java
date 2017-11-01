/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.http.MantaHttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Base class for classes which implement recursive directory creation.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
abstract class RecursiveDirectoryCreationStrategy {

    @SuppressWarnings("checkstyle:JavaDocVariable")
    private static final Logger LOG = LoggerFactory.getLogger(RecursiveDirectoryCreationStrategy.class);

    /**
     * The client to use when performing operations.
     */
    private final MantaClient client;

    /**
     * The number of PUT operations this instance has performed.
     */
    private int operations;

    RecursiveDirectoryCreationStrategy(final MantaClient client) {
        this.client = client;
        this.operations = 0;
    }

    /**
     * Attempt to create the desired directory recursively.
     *
     * @param rawPath    The fully qualified path of the Manta directory.
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an unexpected error occurs during directory creation.
     */
    abstract void create(String rawPath, MantaHttpHeaders headers) throws IOException;

    MantaClient getClient() {
        return client;
    }

    void incrementOperations() {
        operations++;
    }

    void incrementOperations(final int ops) {
        operations += ops;
    }

    int getOperations() {
        return operations;
    }

    /**
     * Try to create a directory and unpack the error. The Boolean is intentional and acts as a tri-state variable.
     *
     * NULL  = creation failed.
     * TRUE  = a new directory was actually created
     * FALSE = the directory already existed
     *
     * @return whether or not the directory was actually new, or null if it failed to be created
     */
    Boolean createNewDirectory(final String path, final MantaHttpHeaders headers) throws IOException {
        try {
            return client.putDirectory(path, headers);
        } catch (final MantaClientHttpResponseException mchre) {
            if (mchre.getServerCode().equals(MantaErrorCode.DIRECTORY_DOES_NOT_EXIST_ERROR)) {
                return null;
            } else {
                throw mchre;
            }
        }
    }
}
