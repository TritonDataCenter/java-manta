/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.joyent.manta.util.MantaUtils.writeablePrefixPaths;

/**
 * Directory creation strategy which attempts to skip redundant directory creation by omitting `PUT` requests for
 * intermediate directories when creating deeply nested directories.
 *
 * If the number of user directories to be created is greater than {@code skipDepth}, this strategy will
 * start creating directories at {@code skipDepth}. For example, if the skipDepth is 3 and the raw path being created
 * is "/user/stor/foo/bar/baz/quux" then the first three non-system paths ("/user/stor/foo", "/user/stor/foo/bar",
 * and "/user/stor/foo/bar/baz") will be skipped and we will attempt to create "/user/stor/foo/bar/baz/quux" directly.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.7
 */
class DepthSkippingDirectoryCreationStrategy extends RecursiveDirectoryCreationStrategy {

    @SuppressWarnings("checkstyle:JavaDocVariable")
    private static final Logger LOG = LoggerFactory.getLogger(DepthSkippingDirectoryCreationStrategy.class);

    /**
     * Number of user-writeable directories to attempt to skip.
     */
    private final int skipDepth;

    DepthSkippingDirectoryCreationStrategy(final MantaClient client, final int depth) {
        super(client);
        Validate.isTrue(0 < depth, "Directory depth must be greater than 0");
        this.skipDepth = depth;
    }

    @Override
    void create(final String rawPath, final MantaHttpHeaders headers) throws IOException {
        final String[] paths = writeablePrefixPaths(rawPath);

        if (paths.length <= skipDepth) {
            createPessimistically(rawPath, headers);
            return;
        }

        final String assumedExistingDirectory = paths[skipDepth - 1];
        final String maybeNewDirectory = paths[skipDepth];

        if (LOG.isDebugEnabled()) {
            LOG.debug("ASSUME " + assumedExistingDirectory);
        }

        final Boolean redundantPut = createNewDirectory(maybeNewDirectory, headers);
        incrementOperations();

        if (redundantPut == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("FAILED:" + maybeNewDirectory);
            }

            // failed to create directory at the skip depth, proceed normally
            createPessimistically(rawPath, headers);
            return;
        }

        for (int idx = skipDepth + 1; idx < paths.length; idx++) {
            getClient().putDirectory(paths[idx], headers);
            incrementOperations();
        }
    }

    /**
     * In cases where we want to fall back to the regular directory creation strategy, create a
     * PessimisticDirectoryCreationStrategy and count its operations toward our own.
     *
     * @param rawPath The fully qualified path of the Manta directory.
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an unexpected error occurs during directory creation.
     */
    private void createPessimistically(final String rawPath, final MantaHttpHeaders headers) throws IOException {
        final RecursiveDirectoryCreationStrategy fallback = new PessimisticDirectoryCreationStrategy(getClient());
        fallback.create(rawPath, headers);
        incrementOperations(fallback.getOperations());
    }
}
