package com.joyent.manta.client;

import java.io.Serializable;

/**
 * Status enum that indicates the current state of a Manta multipart upload.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public enum MantaMultipartStatus implements Serializable {
    /** Multipart upload is in a unknown state (not yet started is possible). */
    UNKNOWN,
    /** Multipart upload was created and is still in progress. */
    CREATED,
    /** Multipart upload is in the process of being finished and written to disk. */
    COMMITTING,
    /** Multipart upload is in the process of being aborted. */
    ABORTING,
    /** Multipart upload has already finished and the final object was written. */
    COMPLETED,
    /** Multipart upload has been aborted. */
    ABORTED;

    private static final long serialVersionUID = 2798516320088229874L;
}
