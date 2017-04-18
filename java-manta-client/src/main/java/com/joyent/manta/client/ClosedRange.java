/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

/**
 * Interface describing a range (closed).
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface ClosedRange extends Range {

    /**
     * Method to get range start (inclusive).
     *
     * @return range start
     */
    long getStart();

    /**
     * Method to get range end (exclusive).
     *
     * @return range end
     */
    long getEnd();

}
