package com.joyent.test.util;

import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;

import java.io.IOException;

/**
 * Assertions for verifying integration test behavior with Manta.
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaAssert {
    /**
     * Functional wrapper that validates if a given closures throws a
     * {@link MantaClientHttpResponseException} and that it has the
     * correct status code.
     *
     * @param statusCode HTTP status code to verify
     * @param function block to verify
     * @param <R> return type
     * @return return value of underlying block
     */
    public static <R> R assertResponseFailureStatusCode(final int statusCode,
                                                 final MantaFunction<R> function) {
        boolean thrown = false;
        int actualCode = -1;
        R result = null;

        try {
            result = function.apply();
        } catch (MantaClientHttpResponseException e) {
            actualCode = e.getStatusCode();
            thrown = true;
        } catch (IOException e) {
            throw new MantaException(e);
        }

        if (!thrown){
            String msg = String.format("Expected %s to be thrown, but it was not thrown",
                    MantaClientHttpResponseException.class);
            throw new AssertionError(msg);
        } else if (thrown && actualCode != statusCode) {
            String msg = String.format("Expected HTTP status code [%d] was: %d",
                    statusCode, actualCode);
            throw new AssertionError(msg);
        }

        return result;
    }
}
