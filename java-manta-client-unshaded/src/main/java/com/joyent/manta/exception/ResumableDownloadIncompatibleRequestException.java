package com.joyent.manta.exception;

public class ResumableDownloadIncompatibleRequestException extends ResumableDownloadException {

    private static final long serialVersionUID = 7415723473743850334L;

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadIncompatibleRequestException(final String msg) {
        super(msg);
    }

    /**
     * {@inheritDoc}
     */
    public ResumableDownloadIncompatibleRequestException(final String msg, final Throwable cause) {
        super(msg, cause);
    }
}
