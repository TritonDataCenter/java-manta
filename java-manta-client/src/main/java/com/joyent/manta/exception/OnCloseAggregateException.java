/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.exception.ContextedRuntimeException;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Exception class that provides an aggregated view of all the exceptions
 * that happened during a close() operation.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.4.0
 */
public class OnCloseAggregateException extends ContextedRuntimeException {
    private static final long serialVersionUID = -593809028604170624L;

    {
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /**
     * Count of the number of exceptions that have been aggregated.
     */
    private AtomicInteger count = new AtomicInteger(0);

    /**
     * Instantiates ContextedRuntimeException without message or cause.
     * <p>
     * The context information is stored using a default implementation.
     */
    public OnCloseAggregateException() {
    }

    /**
     * Instantiates ContextedRuntimeException with message, but without cause.
     * <p>
     * The context information is stored using a default implementation.
     *
     * @param message the exception message, may be null
     */
    public OnCloseAggregateException(final String message) {
        super(message);
    }

    /**
     * Instantiates ContextedRuntimeException with cause, but without message.
     * <p>
     * The context information is stored using a default implementation.
     *
     * @param cause the underlying cause of the exception, may be null
     */
    public OnCloseAggregateException(final Throwable cause) {
        super(cause);
    }

    /**
     * Instantiates ContextedRuntimeException with cause and message.
     * <p>
     * The context information is stored using a default implementation.
     *
     * @param message the exception message, may be null
     * @param cause   the underlying cause of the exception, may be null
     */
    public OnCloseAggregateException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Instantiates ContextedRuntimeException with cause, message, and ExceptionContext.
     *
     * @param message the exception message, may be null
     * @param cause   the underlying cause of the exception, may be null
     * @param context the context used to store the additional information, null uses default implementation
     */
    public OnCloseAggregateException(final String message, final Throwable cause,
                                     final ExceptionContext context) {
        super(message, cause, context);
    }

    /**
     * Adds an exception to the list of contextual parameters.
     * @param e exception to add
     */
    public void aggregateException(final Exception e) {
        String label = String.format("%03d_exception", count.incrementAndGet());
        setContextValue(label, ExceptionUtils.getStackTrace(e));
    }
}
