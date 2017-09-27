/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import javax.management.DynamicMBean;

/**
 * Interface for objects that can make themselves available as MBeans.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.7
 */
public interface MBeanable {

    /**
     * Provide an MBean to the {@link MantaMBeanSupervisor} that represents this object for
     * registration in JMX. Closing the supervisor will deregister the MBean. Implementations are expected
     * to call {@link MantaMBeanSupervisor#expose(DynamicMBean)}.
     *
     * @param supervisor the {@link MantaMBeanSupervisor} in charge of
     *                   registering and deregistering the representative bean
     */
    void createExposedMBean(MantaMBeanSupervisor supervisor);
}
