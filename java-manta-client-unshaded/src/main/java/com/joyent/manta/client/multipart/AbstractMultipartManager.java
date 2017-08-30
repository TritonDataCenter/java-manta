/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.protocol.HttpContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;

/**
 * <p>Base class providing generic methods useful for {@link MantaMultipartManager}
 * implementations.</p>
 *
 * <p>This class provides a coherent structure for multipart implementations
 * that communicate over the network using the Apache HTTP Client library by
 * providing default mappings for part upload methods to a {@link HttpEntity}
 * upload method.</p>
 *
 * <p>By allowing for UPLOAD and PART to be generic, we can easily create
 * wrapping implementations like {@link EncryptedMultipartManager} that
 * will wrap any base implementation.</p>
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 *
 * @param <UPLOAD> Manta multipart upload object used to manage MPU state
 * @param <PART> Manta multipart upload part object used to manage MPU part state
 */
abstract class AbstractMultipartManager<UPLOAD extends MantaMultipartUpload,
                                        PART extends MantaMultipartUploadPart>
        implements MantaMultipartManager<UPLOAD, PART> {
    @SuppressWarnings("ReturnValueIgnored")
    @Override
    public void validateThatThereAreSequentialPartNumbers(final UPLOAD upload)
            throws IOException, MantaMultipartException {
        Validate.notNull(upload, "Multipart upload object must not be null");

        listParts(upload)
                .sorted()
                .map(MantaMultipartUploadPart::getPartNumber)
                .reduce(1, (memo, value) -> {
                    if (!memo.equals(value)) {
                        MantaMultipartException e = new MantaMultipartException(
                                "Missing part of multipart upload");
                        e.setContextValue("missing_part", memo);
                        e.setContextValue("uploadId",  upload.getId());
                        e.setContextValue("path", upload.getPath());
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
            throw new IllegalArgumentException("Negative or zero part numbers "
                    + "are not valid");
        }

        if (partNumber > getMaxParts()) {
            final String msg = String.format("Part number of [%d] exceeds "
                            + "maximum parts (%d)", partNumber, getMaxParts());
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public PART uploadPart(final UPLOAD upload, final int partNumber,
                           final String contents)
            throws IOException {
        validatePartNumber(partNumber);
        Validate.notNull(contents, "String must not be null");

        HttpEntity entity = new ExposedStringEntity(contents,
                ContentType.APPLICATION_OCTET_STREAM);

        return uploadPart(upload, partNumber, entity, null);
    }

    @Override
    public PART uploadPart(final UPLOAD upload,
                           final int partNumber,
                           final byte[] bytes)
            throws IOException {
        validatePartNumber(partNumber);
        Validate.notNull(bytes, "Byte array must not be null");

        HttpEntity entity = new ExposedByteArrayEntity(bytes, ContentType.APPLICATION_OCTET_STREAM);
        return uploadPart(upload, partNumber, entity, null);
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
        return uploadPart(upload, partNumber, entity, null);
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

        return uploadPart(upload, partNumber, entity, null);
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

        return uploadPart(upload, partNumber, entity, null);
    }

    /**
     * <p>Uploads a single part of a multipart upload using an implementation
     * of the Apache HTTP Client's {@link HttpEntity} interface.</p>
     *
     * <p>Even though all implementations of this class take a reference to
     * {@link MantaClient} as a constructor parameter, the {@link MantaClient}
     * class isn't always the best interface to interact with to specify
     * how a multipart upload part is sent to Manta. The abstraction of
     * {@link MantaClient} is intended for users of the SDK and not necessarily
     * internal consumers, so additional customization of HTTP calls needs to
     * be done by a {@link MantaMultipartManager}.</p>
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param entity Apache HTTP Client entity instance
     * @param context additional request context, may be null
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    abstract PART uploadPart(UPLOAD upload,
                             int partNumber,
                             HttpEntity entity,
                             HttpContext context) throws IOException;

    /**
     * <p>Uses reflection to read a private field from a {@link MantaClient}
     * instance.</p>
     *
     * <p>We use reflection to read private fields from {@link MantaClient} as
     * part of a compromise between package level separation and private/default
     * scoping. Essentially, it makes sense to put multipart related classes
     * in their own package because the package in which {@link MantaClient} is
     * contained is already crowded. However, by making that decision there is
     * no scoping mechanism available in Java to allow us to share
     * methods/fields between packages without giving other packages access.
     * Thus, we scope the methods/fields that shouldn't be available to a user
     * of the SDK as private/protected/default and use reflection
     * <em>sparingly</em> to access the values from another package. Particular
     * care has been paid to making this reflection-based reads outside of
     * performance sensitive code paths.</p>
     *
     * @param fieldName field name to read
     * @param mantaClient Manta client instance to read fields from
     * @param returnType type of field
     * @param <T> field type
     * @return value of the field
     */
    protected static <T> T readFieldFromMantaClient(final String fieldName,
                                                    final MantaClient mantaClient,
                                                    final Class<T> returnType) {
        final Field field = FieldUtils.getField(MantaClient.class,
                fieldName, true);

        try {
            Object object = FieldUtils.readField(field, mantaClient, true);
            return returnType.cast(object);
        } catch (IllegalAccessException e) {
            throw new MantaMultipartException("Unable to access httpHelper "
                    + "field on MantaClient");
        }
    }
}
