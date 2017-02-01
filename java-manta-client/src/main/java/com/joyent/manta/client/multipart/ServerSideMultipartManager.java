package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Class providing a server-side natively supported implementation
 * of multipart uploads.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0 */
public class ServerSideMultipartManager implements MantaMultipartManager {
    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(String path) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(String path, MantaMetadata mantaMetadata) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(String path, MantaMetadata mantaMetadata, MantaHttpHeaders httpHeaders) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, String contents) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, File file) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(MantaMultipartUpload upload, int partNumber, InputStream inputStream) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart getPart(MantaMultipartUpload upload, int partNumber) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartStatus getStatus(MantaMultipartUpload upload) throws IOException {
        return null;
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(MantaMultipartUpload upload) throws IOException {
        return null;
    }

    @Override
    public void validateThatThereAreSequentialPartNumbers(MantaMultipartUpload upload) throws IOException, MantaMultipartException {

    }

    @Override
    public void abort(MantaMultipartUpload upload) throws IOException {

    }

    @Override
    public void complete(MantaMultipartUpload upload, Iterable<? extends MantaMultipartUploadTuple> parts) throws IOException {

    }

    @Override
    public void complete(MantaMultipartUpload upload, Stream<? extends MantaMultipartUploadTuple> partsStream) throws IOException {

    }

    @Override
    public <R> R waitForCompletion(MantaMultipartUpload upload, Function<UUID, R> executeWhenTimesToPollExceeded) throws IOException {
        return null;
    }

    @Override
    public <R> R waitForCompletion(MantaMultipartUpload upload, Duration pingInterval, int timesToPoll, Function<UUID, R> executeWhenTimesToPollExceeded) throws IOException {
        return null;
    }
}
