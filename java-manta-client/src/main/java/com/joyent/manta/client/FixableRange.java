/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

/**
 * Interface describing a range (fixable).
 *
 * @param <T> type of range
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface FixableRange<T extends Range> extends Range {

    /**
     * Method to fix range.
     *
     * @param length length of containing object
     *
     * @return fixed range
     */
    T doFix(long length);

}
