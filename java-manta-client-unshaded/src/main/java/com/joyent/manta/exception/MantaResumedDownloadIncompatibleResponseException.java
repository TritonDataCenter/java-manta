package com.joyent.manta.exception;

public class MantaResumedDownloadIncompatibleResponseException extends MantaClientHttpResponseException {
    private static final long serialVersionUID = 123778476650917899L;

    public MantaResumedDownloadIncompatibleResponseException(final String msg) {
        super(msg);
    }
}
