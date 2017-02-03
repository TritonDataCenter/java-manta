package com.joyent.manta.client.multipart;

import java.io.Serializable;
import java.util.Comparator;
import java.util.UUID;

/**
 * Interface representing a multipart upload in progress.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 2.5.0
 */
public interface MantaMultipartUpload extends Comparator<MantaMultipartUpload>, Serializable {
    /**
     * @return upload id for the entire multipart upload operation
     */
    UUID getId();

    /**
     * @return path to the final object
     */
    String getPath();
}
