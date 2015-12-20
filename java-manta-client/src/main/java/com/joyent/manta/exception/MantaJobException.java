/**
 * Copyright (c) 2015, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.exception;

import com.joyent.manta.client.MantaJobError;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Exception thrown when there is a problem processing a Manta job.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaJobException extends MantaException {
    /**
     * Errors associated with failed inputs.
     */
    private final List<MantaJobError> errors;

    /**
     * Job ID associated with this exception.
     */
    private final UUID jobId;


    /**
     * Creates an exception without an associated error list nor a job id.
     * @param message error message
     */
    public MantaJobException(final String message) {
        super(message);
        this.jobId = null;
        this.errors = Collections.emptyList();
    }


    /**
     * Creates an exception without an associated error list.
     * @param jobId job ID
     * @param message error message
     */
    public MantaJobException(final UUID jobId, final String message) {
        super(String.format("[job: %s] %s", jobId, message));
        this.jobId = jobId;
        this.errors = Collections.emptyList();
    }


    /**
     * Creates an exception without an associated error list.
     *
     * @param jobId job ID
     * @param message error message
     * @param cause exception to wrap
     */
    public MantaJobException(final UUID jobId, final String message,
                             final Throwable cause) {
        super(String.format("[job: %s] %s", jobId, message), cause);
        this.jobId = jobId;
        this.errors = Collections.emptyList();
    }


    /**
     * Creates an exception that bundles all of the errors associated with
     * all of the inputs for a job.
     * @param jobId job ID
     * @param errors list of errors for each failed input
     */
    public MantaJobException(final UUID jobId,
                             final List<MantaJobError> errors) {
        this.jobId = jobId;
        this.errors = errors;
    }


    /**
     * @return list of errors for each failed input
     */
    public List<MantaJobError> getErrors() {
        return errors;
    }


    /**
     * @return job id of job that failed
     */
    public UUID getJobId() {
        return jobId;
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
            builder.append(" ").append(msg);

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
