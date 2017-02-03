/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.exception.MantaMultipartException;
import org.apache.commons.lang3.Validate;

import java.io.IOException;

/**
 * Base class providing generic methods useful for {@link MantaMultipartManager}
 * implementations.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <UPLOAD> Manta multipart upload object used to manage MPU state
 * @param <PART> Manta multipart upload part object used to manage MPU part state
 * @param <COMPLETE> Return type for the <code>complete()</code> methods
 */
abstract class AbstractMultipartManager<UPLOAD extends MantaMultipartUpload, PART extends MantaMultipartUploadPart, COMPLETE>
        implements MantaMultipartManager<UPLOAD, PART, COMPLETE> {
    @Override
    public void validateThatThereAreSequentialPartNumbers(final UPLOAD upload)
            throws IOException, MantaMultipartException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        //noinspection ResultOfMethodCallIgnored
        listParts(upload)
                .sorted()
                .map(MantaMultipartUploadPart::getPartNumber)
                .reduce(1, (memo, value) -> {
                    if (!memo.equals(value)) {
                        MantaMultipartException e = new MantaMultipartException(
                                "Missing part of multipart upload");
                        e.setContextValue("missing_part", memo);
                        throw e;
                    }

                    return memo + 1;
                });
    }
}
