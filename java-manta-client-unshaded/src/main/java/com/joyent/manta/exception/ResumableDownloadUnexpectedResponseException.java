package com.joyent.manta.exception;

/**
 * Exception signaling that a resumed download request cannot be continued because the response was invalid.
 * TODO: Do we actually need a new exception?
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public class ResumableDownloadUnexpectedResponseException extends ResumableDownloadException {

    private static final long serialVersionUID = 123778476650917899L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadUnexpectedResponseException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadUnexpectedResponseException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
