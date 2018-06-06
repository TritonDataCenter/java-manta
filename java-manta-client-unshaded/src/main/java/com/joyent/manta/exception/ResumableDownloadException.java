package com.joyent.manta.exception;

public class ResumableDownloadException extends MantaIOException {
    private static final long serialVersionUID = -5972256969855482635L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
