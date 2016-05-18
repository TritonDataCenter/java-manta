/**
 * Copyright (c) 2016, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;

import java.io.IOException;

/**
 * Implementation of {@link HttpIOExceptionHandler} that retries IOExceptions.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class MantaIOExceptionHandler implements HttpIOExceptionHandler {
    /**
     * Creates a new instance.
     */
    public MantaIOExceptionHandler() {
    }

    @Override
    public boolean handleIOException(final HttpRequest request,
                                     final boolean supportsRetry) throws IOException {
        return supportsRetry;
    }
}
