/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.MantaInputStreamEntity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A multipart implementation that entirely uses the local filesystem.
 */
public class TestMultipartManager
        extends AbstractMultipartManager<TestMultipartUpload, MantaMultipartUploadPart> {
    private final File destinationDirectory;
    private final File partsDirectory;

    public TestMultipartManager() {
        try {
            this.partsDirectory = Files.createTempDirectory("multipart").toFile();
            FileUtils.forceDeleteOnExit(partsDirectory);
            this.destinationDirectory = Files.createTempDirectory("multipart-final").toFile();
            FileUtils.forceDeleteOnExit(destinationDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getMaxParts() {
        return 500;
    }

    @Override
    public int getMinimumPartSize() {
        return 1;
    }


    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return null;
    }

    @Override
    public TestMultipartUpload initiateUpload(String path) throws IOException {
        return initiateUpload(path, null);
    }

    @Override
    public TestMultipartUpload initiateUpload(String path, MantaMetadata mantaMetadata) throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    @Override
    public TestMultipartUpload initiateUpload(String path,
                                              MantaMetadata mantaMetadata,
                                              MantaHttpHeaders httpHeaders)
            throws IOException {
        return initiateUpload(path, null, mantaMetadata, httpHeaders);
    }

    @Override
    public TestMultipartUpload initiateUpload(String path,
                                              Long contentLength,
                                              MantaMetadata mantaMetadata,
                                              MantaHttpHeaders httpHeaders) throws IOException {
        File file = new File(destinationDirectory + path);
        file.mkdirs();

        File metadata = new File(destinationDirectory + path + File.separator + "metadata");
        File headers = new File(destinationDirectory + path + File.separator + "headers");
        File contents = new File(destinationDirectory + path + File.separator + "contents");

        FileUtils.forceDeleteOnExit(file);

        if (headers != null) {
            MantaObjectMapper.INSTANCE.writeValue(headers, httpHeaders);
        }

        if (metadata != null) {
            MantaObjectMapper.INSTANCE.writeValue(metadata, mantaMetadata);
        }

        UUID id = UUID.randomUUID();
        return new TestMultipartUpload(id, path, contentLength,
                this.partsDirectory, this.destinationDirectory,
                metadata, headers, contents);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(TestMultipartUpload upload, int partNumber, String contents) throws IOException {
        UUID partId = UUID.randomUUID();
        File part = new File(upload.getPartsPath() + File.separator + partNumber);
        File partIdFile = new File(part.getPath() + ".id");
        FileUtils.write(partIdFile, partId.toString(), StandardCharsets.UTF_8);
        FileUtils.write(part, contents, StandardCharsets.UTF_8);

        return new MantaMultipartUploadPart(partNumber, upload.getPath(), partId.toString());
    }

    @Override
    public MantaMultipartUploadPart uploadPart(TestMultipartUpload upload, int partNumber, byte[] bytes) throws IOException {
        UUID partId = UUID.randomUUID();
        File part = new File(upload.getPartsPath() + File.separator + partNumber);
        File partIdFile = new File(part.getPath() + ".id");
        FileUtils.write(partIdFile, partId.toString(), StandardCharsets.UTF_8);
        FileUtils.writeByteArrayToFile(part, bytes);

        return new MantaMultipartUploadPart(partNumber, upload.getPath(), partId.toString());
    }

    @Override
    public MantaMultipartUploadPart uploadPart(TestMultipartUpload upload, int partNumber, File file) throws IOException {
        UUID partId = UUID.randomUUID();
        File part = new File(upload.getPartsPath() + File.separator + partNumber);
        File partIdFile = new File(part.getPath() + ".id");
        FileUtils.write(partIdFile, partId.toString(), StandardCharsets.UTF_8);
        FileUtils.copyFile(file, part);

        return new MantaMultipartUploadPart(partNumber, upload.getPath(), partId.toString());
    }

    @Override
    public MantaMultipartUploadPart uploadPart(TestMultipartUpload upload,
                                               int partNumber,
                                               InputStream inputStream)
            throws IOException {
        HttpEntity entity = new MantaInputStreamEntity(inputStream,
                ContentType.APPLICATION_OCTET_STREAM);

        return uploadPart(upload, partNumber, entity);
    }

    @Override
    public MantaMultipartUploadPart uploadPart(TestMultipartUpload upload,
                                               int partNumber,
                                               long contentLength,
                                               InputStream inputStream)
            throws IOException {
        return uploadPart(upload, partNumber, inputStream);
    }

    @Override
    protected MantaMultipartUploadPart uploadPart(TestMultipartUpload upload,
                                                  int partNumber,
                                                  HttpEntity entity) throws IOException {
        UUID partId = UUID.randomUUID();
        File part = new File(upload.getPartsPath() + File.separator + partNumber);
        File partIdFile = new File(part.getPath() + ".id");
        FileUtils.write(partIdFile, partId.toString(), StandardCharsets.UTF_8);

        try (FileOutputStream fout = new FileOutputStream(part)) {
            entity.writeTo(fout);
        }

        return new MantaMultipartUploadPart(partNumber, upload.getPath(), partId.toString());
    }

    @Override
    public MantaMultipartUploadPart getPart(TestMultipartUpload upload, int partNumber) throws IOException {
        File part = new File(upload.getPartsPath() + File.separator + partNumber);
        File partIdFile = new File(part.getPath() + ".id");
        String partId = FileUtils.readFileToString(partIdFile, StandardCharsets.UTF_8);

        return new MantaMultipartUploadPart(partNumber, upload.getPath(), partId);
    }

    @Override
    public MantaMultipartStatus getStatus(TestMultipartUpload upload) throws IOException {
        return null;
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(TestMultipartUpload upload) throws IOException {
        return Stream.of(upload.getPartsPath().list()).map(partNumber -> {
            try {
                File part = new File(upload.getPartsPath() + File.separator + partNumber);
                File partIdFile = new File(part.getPath() + ".id");
                String etag = FileUtils.readFileToString(partIdFile, StandardCharsets.UTF_8);
                return new MantaMultipartUploadPart(Integer.parseInt(partNumber), upload.getPath(), etag);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void abort(TestMultipartUpload upload) throws IOException {
        FileUtils.deleteDirectory(upload.getPartsPath());
    }

    @Override
    public void complete(TestMultipartUpload upload, Iterable<? extends MantaMultipartUploadTuple> parts) throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final Stream<? extends MantaMultipartUploadTuple> partsStream =
                StreamSupport.stream(parts.spliterator(), false);

        complete(upload, partsStream);
    }

    @Override
    public void complete(TestMultipartUpload upload, Stream<? extends MantaMultipartUploadTuple> partsStream) throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        File objectContents = upload.getContents();

        try (Stream<? extends MantaMultipartUploadTuple> sorted = partsStream.sorted();
             FileOutputStream fout = new FileOutputStream(objectContents)) {
            sorted.forEach(tuple -> {
                final int partNumber = tuple.getPartNumber();
                File part = new File(upload.getPartsPath() + File.separator + partNumber);

                try (FileInputStream fin = new FileInputStream(part)) {
                    IOUtils.copy(fin, fout);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        if (upload.getContentLength() != null) {
            Validate.isTrue(upload.getContentLength().equals(objectContents.length()),
                    "Completed object's content length [%d] didn't equal expected length [%d]",
                    objectContents.length(), upload.getContentLength());
        }
    }

    public File getDestinationDirectory() {
        return destinationDirectory;
    }

    public File getPartsDirectory() {
        return partsDirectory;
    }
}
