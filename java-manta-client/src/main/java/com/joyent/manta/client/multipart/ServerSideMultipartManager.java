package com.joyent.manta.client.multipart;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.Validate;

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
 * @since 3.0.0
 */
public class ServerSideMultipartManager implements MantaMultipartManager {

    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final String contents)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final byte[] bytes)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final File file) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final MantaMultipartUpload upload,
                                               final int partNumber,
                                               final InputStream inputStream)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart getPart(final MantaMultipartUpload upload,
                                            final int partNumber) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartStatus getStatus(final MantaMultipartUpload upload)
            throws IOException {
        return null;
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(final MantaMultipartUpload upload)
            throws IOException {
        return null;
    }

    @Override
    public void validateThatThereAreSequentialPartNumbers(final MantaMultipartUpload upload)
            throws IOException, MantaMultipartException {

    }

    @Override
    public void abort(final MantaMultipartUpload upload) throws IOException {

    }

    @Override
    public void complete(final MantaMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {

    }

    @Override
    public void complete(final MantaMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {

    }

    @Override
    public <R> R waitForCompletion(final MantaMultipartUpload upload,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        return null;
    }

    @Override
    public <R> R waitForCompletion(final MantaMultipartUpload upload,
                                   final Duration pingInterval,
                                   final int timesToPoll,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        return null;
    }

    /**
     * Creates the JSON request body used to create a new multipart upload request.
     *
     * @param objectPath path to the object on Manta
     * @param mantaMetadata metadata associated with object
     * @param headers HTTP headers associated with object
     *
     * @return byte array containing JSON data
     */
    byte[] createMpuRequestBody(final String objectPath,
                                final MantaMetadata mantaMetadata,
                                final MantaHttpHeaders headers) {
        Validate.notNull(objectPath, "Path to Manta object must not be null");

        CreateMPURequestBody requestBody = new CreateMPURequestBody(
                objectPath, mantaMetadata, headers);

        try {
            return MantaObjectMapper.INSTANCE.writeValueAsBytes(requestBody);
        } catch (IOException e) {
            String msg = "Error serializing JSON for MPU request body";
            throw new MantaMultipartException(msg, e);
        }
    }
}
