/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.http.MantaHttpHeaders;

/**
 * Utility class for recursive directory creation strategies.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celayac</a>
 * @since 3.1.7
 */
final class PruneEmpytParentDirectoryStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(PruneEmpytParentDirectoryStrategy.class);

    private PruneEmpytParentDirectoryStrategy() {
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
        int actualLimit = limit;
        if (actualLimit < 0) {
            actualLimit = path.split(MantaClient.SEPARATOR).length - 1;
        }
        LOG.info("Actual Number : " + actualLimit + " of directories ");
        String curPath = path;
        // We are going to walk backward
        ArrayList<String> dirs = new ArrayList<String>();

        // I am generating all of the possible paths first, then we will delete them.
        String parent = curPath;
        for (int i = 0; i < actualLimit; i++) {
            parent = parent.substring(0, parent.lastIndexOf("/"));
            dirs.add(parent);
        }
        try {
            LOG.info("Number of DIRECTORIES : " + dirs.size() + " of directories ");
            client.delete(dirs, headers);
        } catch (MantaClientHttpResponseException responseException) {
            if (responseException.getServerCode() != MantaErrorCode.DIRECTORY_NOT_EMPTY_ERROR) {
                throw responseException;
            }
        }
    }
}
