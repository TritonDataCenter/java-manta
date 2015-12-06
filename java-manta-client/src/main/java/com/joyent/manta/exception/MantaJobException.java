/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

import com.joyent.manta.client.MantaJobError;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobException extends MantaException {
    private final List<MantaJobError> errors;

    public MantaJobException(String message) {
        super(message);
        this.errors = Collections.emptyList();
    }

    public MantaJobException(List<MantaJobError> errors) {
        this.errors = errors;
    }

    public List<MantaJobError> getErrors() {
        return errors;
    }

    @Override
    public String getMessage() {
        if (errors.isEmpty()) {
            return super.getMessage();
        }

        StringBuilder builder = new StringBuilder();

        Iterator<MantaJobError> itr = errors.iterator();

        while (itr.hasNext()) {
            MantaJobError error = itr.next();
            String msg = String.format("[%s] (phase: %s) %s",
                    error.getCode(), error.getPhase(), error.getMessage());
            builder.append(msg);

            if (itr.hasNext()) {
                builder.append(msg);
            }
        }

        return builder.toString();
    }

    @Override
    public String getLocalizedMessage() {
        if (errors.isEmpty()) {
            return super.getLocalizedMessage();
        }

        return getMessage();
    }
}
