/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

/**
 * Interface for objects that can make themselves available as MBeans.
 */
public interface MBeanable {

    /**
     * Provide a bean to the {@link MantaMBeanSupervisor} that represents this object.
     *
     * @param supervisor the {@link MantaMBeanSupervisor} in charge of
     *                   registering and deregistering the representative bean
     */
    void createExposedMBean(MantaMBeanSupervisor supervisor);
}
