/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

/**
 * Interface indicating a class has access to a {@link MantaConnectionContext}.
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.1.7
 */
interface HttpConnectionAware {

    /**
     * Retrieve the attached {@link MantaConnectionContext}.
     *
     * @return the context
     */
    MantaConnectionContext getConnectionContext();

    /**
     * HTTP Request creation object.
     * @return request creation object for use with the above connection
     */
    MantaHttpRequestFactory getRequestFactory();
}
