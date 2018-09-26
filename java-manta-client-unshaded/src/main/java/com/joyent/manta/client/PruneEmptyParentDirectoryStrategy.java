/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaUtils;

/**
 * Utility class for recursive directory removal strategies.
 *
 * @author <a href="https://github.com/douglasAtJoyent">Douglas Anderson</a>
 * @since 3.2.4
 */
final class PruneEmptyParentDirectoryStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PruneEmptyParentDirectoryStrategy.class);

    /**
     * Pass this if you want to prune all parent directories.
     */
    public static final Integer PRUNE_ALL_PARENTS = -1;

    private PruneEmptyParentDirectoryStrategy() {
    }

    /**
     * This will delete empty parent directories up to the root.
     *
     * @param client - A valid client.
     * @param path - THe complete path that you want to start with.
     * @param limit - The limit of directories to delete.
     * @throws IOException - this is thrown from
     */
    static void pruneParentDirectories(final MantaClient client,
            final MantaHttpHeaders headers,
            final String path,
            final int limit) throws IOException {
        // First thing first, delete the child directory.

        if (LOG.isDebugEnabled()) {
            LOG.debug("Pruning first directory: {}", path);
        }
        client.delete(path, headers, null);

        // Generating all parent paths.
        String[] directories = MantaUtils.writeablePrefixPaths(path);
        ArrayUtils.reverse(directories);
        // Removing first element, because we have already deleted it.
        directories = ArrayUtils.remove(directories, 0);

        int actualLimit = limit;
        if (actualLimit > directories.length || actualLimit == PRUNE_ALL_PARENTS) {
            actualLimit = directories.length;
        }
        for (int i = 0; i < actualLimit; i++) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Pruning directory[{}] at path: {}", i, directories[i]);
                }

                client.delete(directories[i], headers, null);
            } catch (MantaClientHttpResponseException responseException) {
                if (responseException.getServerCode().equals(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR)) {
                    continue;
                } else if (responseException.getServerCode() != MantaErrorCode.DIRECTORY_NOT_EMPTY_ERROR) {
                    throw responseException;
                }

            }
        }
    }
}
