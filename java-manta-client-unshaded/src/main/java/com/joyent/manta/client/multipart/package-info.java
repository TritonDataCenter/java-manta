/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * <p>Package containing all of the classes used as part of the Manta Multipart
 * upload API.</p>
 *
 * <p>This package contains two very different implementations for doing
 * multipart upload:</p>
 * <ul>
 *     <li>Jobs-based multipart - this performs multipart uploads using Manta
 *         jobs to concatenate the uploaded parts. This is a legacy
 *         implementation for Manta deployments that do not yet support
 *         server-side natively supported multipart uploads.</li>
 *     <li>Server side multipart - this performs multipart uploads using Manta's
 *         native support for multipart uploads. If the Manta deployment that
 *         you are connecting to supports this mode, then you should use
 *         this implementation.</li>
 * </ul>
 *
 * <p>There are also abstract classes and interfaces that allow you to
 * implement your own API compatible implementation to use for testing.
 * Additionally, there are encryption implementations that allow for
 * the wrapping of a backing multipart implementation and seamlessly
 * encrypting the uploaded parts.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
package com.joyent.manta.client.multipart;
