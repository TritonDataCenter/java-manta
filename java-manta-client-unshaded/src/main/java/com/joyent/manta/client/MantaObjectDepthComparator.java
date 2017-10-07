/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * {@link Comparator} implementation that sorts {@link MantaObject} instances
 * by file first, then directory depth (deepest first).
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.7
 */
public final class MantaObjectDepthComparator implements Comparator<MantaObject> {
    /**
     * Singleton instance of comparator.
     */
    public static final MantaObjectDepthComparator INSTANCE = new MantaObjectDepthComparator();

    /**
     * Creates a new comparator instance.
     */
    private MantaObjectDepthComparator() {
    }

    @Override
    public int compare(final MantaObject o1, final MantaObject o2) {
        final int depthComparison = comparePathDepth(o1, o2);

        if (depthComparison == 0) {
            if (!o1.isDirectory() == !o2.isDirectory()) {
                return 0;
            } else if (!o1.isDirectory()) {
                return -1;
            } else {
                return 1;
            }
        }

        return depthComparison;
    }

    /**
     * Sorts two objects showing the deepest objects first.
     *
     * @param o1 the first object to be compared.
     * @param o2 the second object to be compared.
     * @return a negative integer, zero, or a positive integer as the
     *         first argument is less than, equal to, or greater than the
     *         second.
     */
    private static int comparePathDepth(final MantaObject o1, final MantaObject o2) {
        final int o1Depth = StringUtils.countMatches(o1.getPath(), SEPARATOR);
        final int o2Depth = StringUtils.countMatches(o2.getPath(), SEPARATOR);
        // Note: reversed order - bigger values should come first
        return Integer.compare(o2Depth, o1Depth);
    }
}
