package com.joyent.manta.exception;

/**
 * Exception signaling that a resumed download request cannot be continued because the response was invalid.
 * TODO: Do we actually need a new exception?
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public class MantaResumedDownloadIncompatibleResponseException extends MantaClientHttpResponseException {

    private static final long serialVersionUID = 123778476650917899L;

    /**
     * Constructs an instance with the specified detail message.
     *
     * @param msg error message
     */
    public MantaResumedDownloadIncompatibleResponseException(final String msg) {
        super(msg);
    }
}
