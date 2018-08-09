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

import static com.joyent.manta.util.MantaUtils.writeablePrefixPaths;

/**
 * Utility class for recursive directory creation strategies.
 *
 * @author <a href="hhttps://github.com/douglasAtJoyent">Douglas Anderson</a>
 * @since 3.2.4
 */
final class RecursiveDirectoryCreationStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(RecursiveDirectoryCreationStrategy.class);

    private RecursiveDirectoryCreationStrategy() {
    }

    static long createWithSkipDepth(final MantaClient client,
                                    final String rawPath,
                                    final MantaHttpHeaders headers,
                                    final int skipDepth) throws IOException {
        final String[] paths = writeablePrefixPaths(rawPath);

        if (paths.length <= skipDepth) {
            return createCompletely(client, rawPath, headers);
        }

        final String assumedExistingDirectory = paths[skipDepth - 1];
        final String maybeNewDirectory = paths[skipDepth];

        LOG.debug("ASSUME {}", assumedExistingDirectory);

        final Boolean redundantPut = createNewDirectory(client, maybeNewDirectory, headers, rawPath);
        long ops = 1;

        if (redundantPut == null) {
            LOG.debug("FAILED {}", maybeNewDirectory);

            // failed to create directory at the skip depth, proceed normally
            return ops + createCompletely(client, rawPath, headers);
        }

        for (int idx = skipDepth + 1; idx < paths.length; idx++) {
            client.putDirectory(paths[idx], headers);
            ops++;
        }

        return ops;
    }

    static long createCompletely(final MantaClient client,
                                 final String rawPath,
                                 final MantaHttpHeaders headers) throws IOException {
        long ops = 0;
        for (final String path : writeablePrefixPaths(rawPath)) {
            client.putDirectory(path, headers);
            ops++;
        }

        return ops;
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
    private static Boolean createNewDirectory(final MantaClient client,
                                              final String path,
                                              final MantaHttpHeaders headers,
                                              final String targetPath) throws IOException {
        try {
            return client.putDirectory(path, headers);
        } catch (final MantaClientHttpResponseException mchre) {
            if (mchre.getServerCode().equals(MantaErrorCode.DIRECTORY_DOES_NOT_EXIST_ERROR)) {
                return null;
            } else {
                mchre.setContextValue("recursiveDirectoryCreationTarget", targetPath);
                throw mchre;
            }
        }
    }
}
