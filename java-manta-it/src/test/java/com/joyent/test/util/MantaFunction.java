package com.joyent.test.util;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;

import java.io.IOException;

/**
 * @author Elijah Zupancic
 * @since 1.0.0
 */
@FunctionalInterface
public interface MantaFunction<R> {

    /**
     * A closure for wrapping sections of code for testing.
     * @return the function result
     */
    R apply() throws MantaClientHttpResponseException, MantaException, IOException;
}
