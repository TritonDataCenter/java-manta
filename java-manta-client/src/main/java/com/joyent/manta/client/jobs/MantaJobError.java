/*
 * Copyright (c) 2015-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.jobs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.joyent.manta.exception.MantaErrorCode;

import java.util.Objects;

/**
 * Details regarding an error that happened when processing a Manta job.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MantaJobError {
    /**
     * Phase number of the failure.
     */
    private int phase;

    /**
     * A human readable summary of what failed.
     */
    private String what;

    /**
     * Input associated with the phase 0.
     */
    private String p0input;

    /**
     * Manta input object associated with error.
     */
    private String input;

    /**
     * A programmatic error code.
     */
    private String code;

    /**
     * Human readable error message.
     */
    private String message;

    /**
     * (optional) a key that saved the stderr for the given command.
     */
    private String stderr;

    /**
     * (optional) the input key being processed when the task failed (if the service can determine it).
     */
    private String key;


    /**
     * Default constructor.
     */
    public MantaJobError() {
    }

    /**
     * @return phase number of the failure
     */
    public int getPhase() {
        return phase;
    }

    /**
     * @return a human readable summary of what failed
     */
    public String getWhat() {
        return what;
    }

    /**
     * @return Input associated with the phase 0
     */
    public String getP0input() {
        return p0input;
    }

    /**
     * @return Manta input object associated with error
     */
    public String getInput() {
        return input;
    }

    /**
     * @return a programmatic error code
     */
    public String getCode() {
        return code;
    }

    /**
     * @return a programmatic error code as a {@link MantaErrorCode} enum
     */
    public MantaErrorCode getMantaErrorCode() {
        return MantaErrorCode.valueOfCode(getCode());
    }

    /**
     * @return human readable error message
     */
    public String getMessage() {
        return message;
    }

    /**
     * @return a key that saved the stderr for the given command
     */
    public String getStderr() {
        return stderr;
    }

    /**
     * @return the input key being processed when the task failed
     */
    public String getKey() {
        return key;
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        MantaJobError that = (MantaJobError) other;
        return phase == that.phase
                && Objects.equals(what, that.what)
                && Objects.equals(p0input, that.p0input)
                && Objects.equals(input, that.input)
                && Objects.equals(code, that.code)
                && Objects.equals(message, that.message)
                && Objects.equals(stderr, that.stderr)
                && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phase, what, p0input, input, code, message, stderr, key);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MantaJobError{");
        sb.append("phase=").append(phase);
        sb.append(", what='").append(what).append('\'');
        sb.append(", p0input='").append(p0input).append('\'');
        sb.append(", input='").append(input).append('\'');
        sb.append(", code='").append(code).append('\'');
        sb.append(", message='").append(message).append('\'');
        sb.append(", stderr='").append(stderr).append('\'');
        sb.append(", key='").append(key).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
