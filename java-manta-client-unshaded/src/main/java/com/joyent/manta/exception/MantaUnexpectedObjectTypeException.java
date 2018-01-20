/*
 * Copyright (c) 2018, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import com.joyent.manta.domain.ObjectType;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.Validate;

/**
 * Exception indicating that an unexpected object type was encountered with a
 * remote object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.2.2
 */
public class MantaUnexpectedObjectTypeException extends MantaClientException {
    private static final long serialVersionUID = -1028368819651385028L;

    /**
     * Expected object type.
     */
    private final ObjectType expected;

    /**
     * Actual object type.
     */
    private final ObjectType actual;

    /**
     * Optional server response headers object.
     */
    private MantaHttpHeaders responseHeaders;

    /**
     * Creates a new instance with the expected properties.
     *
     * @param expected expected object type
     * @param actual actual object type
     */
    public MantaUnexpectedObjectTypeException(final ObjectType expected,
                                              final ObjectType actual) {
        super(msg(expected, actual));
        this.expected = expected;
        this.actual = actual;
        updateContext();
    }

    /**
     * Creates a new instance with the expected properties.
     *
     * @param message the detail message. The detail message is saved for
    *                 later retrieval by the {@link #getMessage()} method.
     * @param expected expected object type
     * @param actual actual object type
     */
    public MantaUnexpectedObjectTypeException(final String message,
                                              final ObjectType expected,
                                              final ObjectType actual) {
        super(message);
        this.expected = expected;
        this.actual = actual;
        updateContext();
    }

    /**
     * Creates a new instance with the expected properties.
     *
     * @param cause the cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A <tt>null</tt> value is
     *              permitted, and indicates that the cause is nonexistent or
     *              unknown.)
     * @param expected expected object type
     * @param actual actual object type
     */
    public MantaUnexpectedObjectTypeException(final Throwable cause,
                                              final ObjectType expected,
                                              final ObjectType actual) {
        super(msg(expected, actual), cause);
        this.expected = expected;
        this.actual = actual;
        updateContext();
    }

    /**
     * Creates a new instance with the expected properties.
     *
     * @param message the detail message. The detail message is saved for
     *                later retrieval by the {@link #getMessage()} method.
     * @param cause the cause (which is saved for later retrieval by the
     *              {@link #getCause()} method).  (A <tt>null</tt> value is
     *              permitted, and indicates that the cause is nonexistent or
     *              unknown.)
     * @param expected expected object type
     * @param actual actual object type
     */
    public MantaUnexpectedObjectTypeException(final String message,
                                              final Throwable cause,
                                              final ObjectType expected,
                                              final ObjectType actual) {
        super(message, cause);
        this.expected = expected;
        this.actual = actual;
        updateContext();
    }

    public ObjectType getExpected() {
        return expected;
    }

    public ObjectType getActual() {
        return actual;
    }

    public MantaHttpHeaders getResponseHeaders() {
        return responseHeaders;
    }

    /**
     * Sets the response headers of the failed request. This is useful for
     * debugging the reason of why the objects didn't match the expectation.
     *
     * @param responseHeaders headers object to attach to exception
     */
    public void setResponseHeaders(final MantaHttpHeaders responseHeaders) {
        this.responseHeaders = responseHeaders;
        this.setContextValue("responseHeaders", responseHeaders.toString());
    }

    /**
     * Updates the exception context with the object type expectations.
     */
    private void updateContext() {
        Validate.notNull(expected, "Expected value should not be null");
        Validate.notNull(actual, "Actual value should not be null");
        Validate.isTrue(!expected.equals(actual),
                "Expected and actual shouldn't be equal. If they "
                    + "were equal - we wouldn't have an exception.");

        setContextValue("expectedObjectType", expected);
        setContextValue("actualObjectType", actual);
    }

    /**
     * Formats a default error message based on the object type expectations.
     *
     * @param expected expected object type
     * @param actual actual object type
     *
     * @return formatted error message
     */
    private static String msg(final ObjectType expected, final ObjectType actual) {
        final String format = "Expected the object type [%s] actually [%s]";
        return String.format(format, expected, actual);
    }
}
