/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
     * Method that will delete the specified path. If the delete of the path is
     * successful, then it will progressively go from right (deepest) to left
     * in the path attempting to delete all parent directories until a non-empty
     * directory is encountered or until a directory is missing.
     *
     * @param client a reference to the manta client instance for the operations
     * @param path file or directory object to delete
     * @param limit limit of directories to delete
     * @throws IOException thrown when there is a problem deleting from Manta
     */
    static void pruneParentDirectories(final MantaClient client,
            final MantaHttpHeaders headers,
            final String path,
            final int limit) throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Pruning first object: {}", path);
        }
        /* The first operation is to delete either a file or a directory
         * at the lowest point. This operation is the minimum criteria for
         * success. If this operation doesn't succeed, we consider the
         * prune operation as a failure. That is why all errors from this
         * operation bubble up. */
        client.delete(path, headers, null);

        /* Listing of all directories within an immutable base path (e.g.
         * /user/stor) with the base path removed. For example, you would see
         * values like: /user/stor/dir1, /user/stor/dir1/dir2... */
        final String[] directories = parseDirectoriesFromPathRightToLeft(path);

        final int actualLimit;

        /* If the limit is higher than the directories specified, it may be a
         * logic error on the part of the user of the SDK, but we have made the
         * decision to not error and instead process all directories available.
         *
         * From a user experience perspective, this makes sense and it allows
         * us to be compatible with the existing behavior in the current
         * release. */
        if (limit > directories.length) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Limit [{}] was set to greater than the number of " +
                        "directories in path [{}]", limit, directories.length);
            }

            actualLimit = directories.length;

        /* If the limit is set to -1 (the prune all parents constant), then
         * we automatically set the limit to the number of prunable
         * directories because it allows the user to not have to think about
         * a specific limit. */
        } else if (limit == PRUNE_ALL_PARENTS) {
            actualLimit = directories.length;
        } else if (limit < PRUNE_ALL_PARENTS) {
            final String msg = String.format("The prune directory limit "
                    + "specified [%d] is less than the minimum value of -1",
                    limit);
            throw new IllegalArgumentException(msg);
        } else {
            actualLimit = limit;
        }

        for (int i = 0; i < actualLimit; i++) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Pruning directory[{}] at path: {}", i, directories[i]);
                }

                client.delete(directories[i], headers, null);
            } catch (MantaClientHttpResponseException responseException) {
                switch (responseException.getServerCode()) {
                    /* The directory to be deleted was deleted by someone else
                     * so we stop pruning. */
                    case RESOURCE_NOT_FOUND_ERROR:
                        LOG.debug("Directory not found: {}", directories[i]);
                        return;
                    /* The directory to be deleted was not empty so we stop
                       pruning. */
                    case DIRECTORY_NOT_EMPTY_ERROR:
                        LOG.debug("Directory not empty: {}", directories[i]);
                        return;
                    default:
                        throw responseException;
                }
            }
        }
    }

    /**
     * Method that will product an array of strings representing each directory
     * in a path ordered from right to left with the deepest object excluded.
     * Additionally, immutable base paths like /user/stor will be excluded
     * from the array of directories.
     *
     * @param path path to extract directories from
     * @return array of directory full paths
     */
    static String[] parseDirectoriesFromPathRightToLeft(final String path) {
        /* Listing of all directories within an immutable base path (e.g.
         * /user/stor) with the base path removed. For example, you would see
         * values like: /user/stor/dir1, /user/stor/dir1/dir2... */
        String[] directories = MantaUtils.writeablePrefixPaths(path);

        // We invert the paths so that we process from the deepest nesting
        ArrayUtils.reverse(directories);

        // Removing first element, because we have already deleted it
        directories = ArrayUtils.remove(directories, 0);

        return directories;
    }
}
