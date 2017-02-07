/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Base class providing generic methods useful for {@link MantaMultipartManager}
 * implementations.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <UPLOAD> Manta multipart upload object used to manage MPU state
 * @param <PART> Manta multipart upload part object used to manage MPU part state
 */
abstract class AbstractMultipartManager<UPLOAD extends MantaMultipartUpload, PART extends MantaMultipartUploadPart>
        implements MantaMultipartManager<UPLOAD, PART> {
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

    /**
     * Validates that the given part number is specified correctly.
     *
     * @param partNumber integer part number value
     * @throws IllegalArgumentException if partNumber is less than 1 or greater than MULTIPART_DIRECTORY
     */
    void validatePartNumber(final int partNumber) {
        if (partNumber <= 0) {
            throw new IllegalArgumentException("Negative or zero part numbers are not valid");
        }

        if (partNumber > getMaxParts()) {
            final String msg = String.format("Part number of [%d] exceeds maximum parts (%d)",
                    partNumber, getMaxParts());
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public PART uploadPart(final UPLOAD upload, final int partNumber, final String contents)
            throws IOException {
        validatePartNumber(partNumber);
        Validate.notNull(contents, "String must not be null");

        HttpEntity entity = new ExposedStringEntity(contents,
                ContentType.APPLICATION_OCTET_STREAM);

        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public PART uploadPart(final UPLOAD upload,
                           final int partNumber,
                           final byte[] bytes)
            throws IOException {
        validatePartNumber(partNumber);
        Validate.notNull(bytes, "Byte array must not be null");

        HttpEntity entity = new ExposedByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public PART uploadPart(final UPLOAD upload,
                           final int partNumber,
                           final File file) throws IOException {
        validatePartNumber(partNumber);
        Validate.notNull(file, "File must not be null");


        if (!file.exists()) {
            String msg = String.format("File doesn't exist: %s",
                    file.getPath());
            throw new FileNotFoundException(msg);
        }

        if (!file.canRead()) {
            String msg = String.format("Can't access file for read: %s",
                    file.getPath());
            throw new IOException(msg);
        }

        HttpEntity entity = new FileEntity(file, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public PART uploadPart(final UPLOAD upload,
                           final int partNumber,
                           final InputStream inputStream)
            throws IOException {
        Validate.notNull("InputStream must not be null");

        if (inputStream.getClass().equals(FileInputStream.class)) {
            final FileInputStream fin = (FileInputStream)inputStream;
            final long contentLength = fin.getChannel().size();
            return uploadPart(upload, partNumber, contentLength, inputStream);
        }

        HttpEntity entity = new MantaInputStreamEntity(inputStream,
                ContentType.APPLICATION_OCTET_STREAM);

        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public PART uploadPart(final UPLOAD upload,
                           final int partNumber,
                           final long contentLength,
                           final InputStream inputStream) throws IOException {
        Validate.notNull("InputStream must not be null");

        HttpEntity entity;

        if (contentLength > -1) {
            entity = new MantaInputStreamEntity(inputStream, contentLength,
                    ContentType.APPLICATION_OCTET_STREAM);
        } else {
            entity = new MantaInputStreamEntity(inputStream,
                    ContentType.APPLICATION_OCTET_STREAM);
        }

        return uploadPart(upload, partNumber, entity);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param entity Apache HTTP Client entity instance
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    abstract PART uploadPart(UPLOAD upload,
                             int partNumber,
                             HttpEntity entity) throws IOException;
}
