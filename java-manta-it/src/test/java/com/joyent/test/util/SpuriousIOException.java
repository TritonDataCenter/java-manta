/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.test.util;

import java.io.IOException;

public class SpuriousIOException extends IOException {

    private static final long serialVersionUID = 229727410007992086L;

    SpuriousIOException(String message) {
        super(message);
    }
}
