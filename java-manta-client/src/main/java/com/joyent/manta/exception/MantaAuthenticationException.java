package com.joyent.manta.exception;

/**
 * Exception class that indicates there was a problem authenticating against the
 * Manta API.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaAuthenticationException extends MantaIOException {

    private static final long serialVersionUID = 3214859017227365745L;

    /**
     * Create a new instance with the default message.
     */
    public MantaAuthenticationException() {
        super("Unable to authenticated against Manta. Check credentials.");
    }
}
