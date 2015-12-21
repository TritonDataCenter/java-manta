package com.joyent.test.util;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;

import java.io.IOException;

/**
 * Function allowing us to make useful testing lamdas.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@FunctionalInterface
public interface MantaFunction<R> {

    /**
     * A closure for wrapping sections of code for testing.
     * @return the function result
     */
    R apply() throws MantaClientHttpResponseException, MantaException, IOException;
}
