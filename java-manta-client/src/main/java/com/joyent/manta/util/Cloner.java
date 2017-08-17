/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.util;

/**
 * Interface indicating a class which is designed to create fully independent clones.
 *
 * @param <T> type to clone
 */
public interface Cloner<T> {

    /**
     * Generates a new {@code T} using values from {@code source}.
     *
     * @param source the source of state to use
     * @return a brand new {@code T}
     */
    T createClone(T source);
}
