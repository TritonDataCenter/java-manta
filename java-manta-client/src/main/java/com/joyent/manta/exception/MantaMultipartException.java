package com.joyent.manta.exception;

/**
 * General exception type for errors relating to multipart uploads.
 *
 * @since 2.5.0
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMultipartException extends MantaClientException {

    private static final long serialVersionUID = -1931282527258322479L;

    /**
     * Create an empty exception.
     */
    public MantaMultipartException() {
    }

    /**
     * @param message The error message.
     */
    public MantaMultipartException(final String message) {
        super(message);
    }

    /**
     * @param cause The cause of the exception.
     */
    public MantaMultipartException(final Throwable cause) {
        super(cause);
    }

    /**
     * @param message The error message.
     * @param cause The cause.
     */
    public MantaMultipartException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
