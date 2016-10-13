package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 * Class providing an API to multi-part uploads to Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMultipart {
    public static final int MAX_PARTS = 10_000;

    static final String MULTIPART_DIRECTORY = "stor/.multipart-6439b444-9041-11e6-9be2-9f622f483d01";

    /**
     * Reference to {@link MantaClient} Manta client object providing access to Manta.
     */
    final MantaClient mantaClient;

    /**
     * Reference to thread pool executor used for asynchronous requests.
     */
    final ExecutorService executorService;

    public MantaMultipart(final MantaClient mantaClient,
                          final ExecutorService executorService) {
        if (mantaClient == null) {
            throw new IllegalArgumentException("Manta client must be present");
        }
        this.mantaClient = mantaClient;
        this.executorService = executorService;
    }

    public MantaMultipart(final MantaClient mantaClient) {
        this(mantaClient, null);
    }

    /**
     * When true indicates that multi-part upload has already started.
     * @param object path to the final assembled object to be uploaded
     * @return true if the upload has started
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    boolean isStarted(final String object) throws IOException {
        final String dir = multipartUploadDir(object);

        try {
            final MantaObjectResponse response = mantaClient.head(dir);

            if (response.isDirectory()) {
                return true;
            }

            MantaIOException exception = new MantaIOException(
                    "Remote path was a file and not a directory as expected");
            exception.setContextValue("multipartUploadPath", dir);
            throw exception;
        } catch (MantaClientHttpResponseException e) {
            if (e.getServerCode().equals(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR)) {
                return false;
            }

            throw e;
        }
    }

    public MantaObjectResponse putPart(final String object, final int partNumber, final String contents)
            throws IOException {

        final String path = multipartPath(object, partNumber);
        prepareUpload(object);

        return mantaClient.put(path, contents);
    }

    public MantaObjectResponse putPart(final String object, final int partNumber, final byte[] bytes)
            throws IOException {
        final String path = multipartPath(object, partNumber);
        prepareUpload(object);

        return mantaClient.put(path, bytes);
    }

    public MantaObjectResponse putPart(final String object, final int partNumber, final File file)
            throws IOException {
        final String path = multipartPath(object, partNumber);
        prepareUpload(object);

        return mantaClient.put(path, file);
    }

    public MantaObjectResponse putPart(final String object, final int partNumber, final InputStream contents)
            throws IOException {
        final String path = multipartPath(object, partNumber);
        prepareUpload(object);

        return mantaClient.put(path, contents);
    }

    public Stream<String> listParts(final String object) throws IOException {
        final String dir = multipartUploadDir(object);

        return mantaClient.listObjects(dir)
                .map(mantaObject -> Paths.get(mantaObject.getPath()).getFileName().toString());
    }

    public void validateThereAreNoMissingParts(final String object) throws IOException {
        listParts(object)
            .map(Integer::parseInt)
            .sorted()
            .reduce(1, (memo, value) -> {
                if (!memo.equals(value)) {
                    MantaClientException e = new MantaClientException(
                            "Missing part of multi-part upload");
                    e.setContextValue("missing_part", memo);
                    throw e;
                }

                return memo + 1;
            });
    }

    public void abort(final String object) throws IOException {
        final String dir = multipartUploadDir(object);

        mantaClient.deleteRecursive(dir);
    }

    public void complete(final String object) throws IOException {

    }

    public void waitForCompletion(final String object) throws IOException {
        waitForCompletion(object, Duration.ofSeconds(5L));
    }

    public void isComplete(final String object) throws IOException {

    }

    public void waitForCompletion(final String object, final Duration pingInterval) {

    }

    String multipartPath(final String object, final int partNumber) {
        validatePartNumber(partNumber);
        final String dir = multipartUploadDir(object);
        return String.format("%s%d", dir, partNumber);
    }

    /**
     * Finds the directory in which to upload parts into.
     *
     * @param object final object to be written to
     * @return temporary Manta directory in which to upload parts
     */
    String multipartUploadDir(final String object) {
        if (StringUtils.isBlank(object)) {
            throw new IllegalArgumentException("Object path must be present and not blank");
        }

        final ConfigContext config = mantaClient.getContext();

        return config.getMantaHomeDirectory()
                + MantaClient.SEPARATOR + MULTIPART_DIRECTORY
                + object + MantaClient.SEPARATOR;
    }

    void prepareUpload(final String object) throws IOException {
        if (!isStarted(object)) {
            final String uploadDir = multipartUploadDir(object);
            mantaClient.putDirectory(uploadDir, true);
        }
    }

    /**
     * Validates that the given part number
     *
     * @param partNumber integer part number value
     * @throws IllegalArgumentException if partNumber is less than 1 or greater than MULTIPART_DIRECTORY
     */
    static void validatePartNumber(final int partNumber) {
        if (partNumber <= 0) {
            throw new IllegalArgumentException("Negative or zero part numbers are not valid");
        }

        if (partNumber > MAX_PARTS) {
            final String msg = String.format("Part number of [%d] exceeds maximum parts (%d)",
                    partNumber, MAX_PARTS);
            throw new IllegalArgumentException(msg);
        }
    }
}
