/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

/**
 * Abstract class providing reflection helper methods for use with
 * cloning.
 *
 * @param <T> type to clone
 */
public abstract class AbstractCloner<T> {

    /**
     * Generates a new {@code T} using values from {@code original}
     * @param source the source of state to use
     * @return a brand new {@code T}
     */
    public abstract T createClone(final T source);
}
