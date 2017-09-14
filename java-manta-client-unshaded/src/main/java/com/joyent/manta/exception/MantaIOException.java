/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.util.MantaVersion;
import org.apache.commons.lang3.exception.DefaultExceptionContext;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * General IO exception type that all other IO exceptions within
 * the project inherit from.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.2.4
 */
public class MantaIOException extends IOException implements ExceptionContext {
    private static final long serialVersionUID = -936151071556739874L;
    /** The context where the data is stored. */
    private final ExceptionContext exceptionContext;

    /**
     * Constructs an instance with {@code null}
     * as its error detail message.
     */
    public MantaIOException() {
        exceptionContext = new DefaultExceptionContext();
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /**
     * Constructs an instance with the specified detail message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     */
    public MantaIOException(final String message) {
        super(message);
        exceptionContext = new DefaultExceptionContext();
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /**
     * Constructs an instance with the specified detail message
     * and cause.
     *
     * <p> Note that the detail message associated with {@code cause} is
     * <i>not</i> automatically incorporated into this exception's detail
     * message.
     *
     * @param message
     *        The detail message (which is saved for later retrieval
     *        by the {@link #getMessage()} method)
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public MantaIOException(final String message, final Throwable cause) {
        super(message, cause);
        exceptionContext = new DefaultExceptionContext();
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /**
     * Constructs an instance with the specified cause and a
     * detail message of {@code (cause==null ? null : cause.toString())}
     * (which typically contains the class and detail message of {@code cause}).
     * This constructor is useful for IO exceptions that are little more
     * than wrappers for other throwables.
     *
     * @param cause
     *        The cause (which is saved for later retrieval by the
     *        {@link #getCause()} method).  (A null value is permitted,
     *        and indicates that the cause is nonexistent or unknown.)
     */
    public MantaIOException(final Throwable cause) {
        super(cause);
        exceptionContext = new DefaultExceptionContext();
        addContextValue("mantaSdkVersion", MantaVersion.VERSION);
    }

    /* All code below was copied from org.apache.commons.lang3.exception.ContextedException
     * and therefore falls under the Apache 2.0 license: http://www.apache.org/licenses/
     */

    //-----------------------------------------------------------------------
    /**
     * Adds information helpful to a developer in diagnosing and correcting the problem.
     * For the information to be meaningful, the value passed should have a reasonable
     * toString() implementation.
     * Different values can be added with the same label multiple times.
     * <p>
     * Note: This exception is only serializable if the object added is serializable.
     * </p>
     *
     * @param label  a textual label associated with information, {@code null} not recommended
     * @param value  information needed to understand exception, may be {@code null}
     * @return {@code this}, for method chaining, not {@code null}
     */
    @Override
    public ExceptionContext addContextValue(final String label, final Object value) {
        exceptionContext.addContextValue(label, value);
        return this;
    }

    /**
     * Sets information helpful to a developer in diagnosing and correcting the problem.
     * For the information to be meaningful, the value passed should have a reasonable
     * toString() implementation.
     * Any existing values with the same labels are removed before the new one is added.
     * <p>
     * Note: This exception is only serializable if the object added as value is serializable.
     * </p>
     *
     * @param label  a textual label associated with information, {@code null} not recommended
     * @param value  information needed to understand exception, may be {@code null}
     * @return {@code this}, for method chaining, not {@code null}
     */
    @Override
    public ExceptionContext setContextValue(final String label, final Object value) {
        exceptionContext.setContextValue(label, value);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object> getContextValues(final String label) {
        return this.exceptionContext.getContextValues(label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFirstContextValue(final String label) {
        return this.exceptionContext.getFirstContextValue(label);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Pair<String, Object>> getContextEntries() {
        return this.exceptionContext.getContextEntries();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getContextLabels() {
        return exceptionContext.getContextLabels();
    }

    /**
     * Provides the message explaining the exception, including the contextual data.
     *
     * @see java.lang.Throwable#getMessage()
     * @return the message, never null
     */
    @Override
    public String getMessage() {
        return getFormattedExceptionMessage(super.getMessage());
    }

    /**
     * Provides the message explaining the exception without the contextual data.
     *
     * @see java.lang.Throwable#getMessage()
     * @return the message
     * @since 3.0.1
     */
    public String getRawMessage() {
        return super.getMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getFormattedExceptionMessage(final String baseMessage) {
        return exceptionContext.getFormattedExceptionMessage(baseMessage);
    }
}
