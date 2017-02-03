package com.joyent.manta.client.multipart;

import java.util.UUID;

/**
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public class JobsMultipartUpload extends AbstractMultipartUpload {
    /**
     * Creates a new instance associated with the specified id
     * and object path.
     *
     * @param uploadId Transaction ID for multipart upload
     * @param path Path to final object being uploaded to Manta
     */
    public JobsMultipartUpload(final UUID uploadId, final String path) {
        super(uploadId, path);
    }
}
