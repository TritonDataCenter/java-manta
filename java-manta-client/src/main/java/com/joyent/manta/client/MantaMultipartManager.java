package com.joyent.manta.client;

import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaIOException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Class providing an API to multipart uploads to Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMultipartManager {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(HttpHelper.class);

    /**
     * Maximum number of parts for a single Manta object.
     */
    public static final int MAX_PARTS = 10_000;

    /**
     * Temporary storage directory on Manta for multipart data.
     */
    static final String MULTIPART_DIRECTORY =
            "stor/.multipart-6439b444-9041-11e6-9be2-9f622f483d01";

    /**
     * Metadata file containing information about final multipart file.
     */
    static final String METADATA_FILE = "metadata";

    /**
     * Default number of seconds to poll Manta to see if a job is complete.
     */
    private static final long DEFAULT_SECONDS_TO_POLL = 5L;

    /**
     * Number of times to check to see if a multipart transfer has completed.
     */
    private static final int NUMBER_OF_TIMES_TO_POLL = 20;

    /**
     * Reference to {@link MantaClient} Manta client object providing access to
     * Manta.
     */
    private final MantaClient mantaClient;

    /**
     * Full path on Manta to the upload directory.
     */
    private final String resolvedMultipartUploadDirectory;

    /**
     * Creates a new instance backed by the specified {@link MantaClient}.
     * @param mantaClient Manta client instance to use to communicate with server
     */
    public MantaMultipartManager(final MantaClient mantaClient) {
        if (mantaClient == null) {
            throw new IllegalArgumentException("Manta client must be present");
        }
        this.mantaClient = mantaClient;

        this.resolvedMultipartUploadDirectory =
                mantaClient.getContext().getMantaHomeDirectory()
                + SEPARATOR + MULTIPART_DIRECTORY;
    }

    /**
     * Lists multipart uploads that are currently in progress.
     *
     * @return stream of objects representing files being uploaded via multipart
     * @throws IOException thrown when there are network issues
     */
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        final Stream<MantaObject> multipartDirList;

        try {
            multipartDirList = mantaClient
                    .listObjects(this.resolvedMultipartUploadDirectory)
                    .filter(MantaObject::isDirectory);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return Stream.empty();
            } else {
                throw e;
            }
        }

        final List<Throwable> exceptions = new ArrayList<>();

        final Stream<MantaMultipartUpload> stream = multipartDirList
                .map(object -> {
                    String idString = MantaUtils.lastItemInPath(object.getPath());
                    UUID id = UUID.fromString(idString);

                    try {
                        MultipartMetadata mantaMetadata = downloadMultipartMetadata(id);
                        return new MantaMultipartUpload(id, mantaMetadata.getPath());
                    } catch (MantaClientHttpResponseException e) {
                        if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                            return null;
                        } else {
                            exceptions.add(e);
                            return null;
                        }
                    } catch (IOException | RuntimeException e) {
                        exceptions.add(e);
                        return null;
                    }
                })
            /* We explicitly filter out items that stopped existing when we
             * went to get the multipart metadata because we encountered a
             * race condition. */
            .filter(value -> value != null);

        if (exceptions.isEmpty()) {
            return stream;
        }

        final MantaIOException aggregateException = new MantaIOException(
                "Problem(s) listing multipart uploads in progress");

        int count = 1;
        for (Throwable e : exceptions) {
            final String label = String.format("exception_%d", count++);
            aggregateException.setContextValue(label, e);
        }

        throw aggregateException;
    }

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @return unique id for the multipart upload
     * @throws IOException thrown when there are network issues
     */
    public MantaMultipartUpload initiateUpload(final String path) throws IOException {
        return initiateUpload(path, null, null);
    }

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @param mantaMetadata metadata to write to final Manta object
     * @return unique id for the multipart upload
     * @throws IOException thrown when there are network issues
     */
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata) throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    /**
     * Initializes a new multipart upload for an object.
     *
     * @param path remote path of Manta object to be uploaded
     * @param mantaMetadata metadata to write to final Manta object
     * @param httpHeaders HTTP headers to read from to write to final Manta object
     * @return unique id for the multipart upload
     * @throws IOException thrown when there are network issues
     */
    public MantaMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders) throws IOException {
        final UUID id = UUID.randomUUID();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating a new multipart upload [{}] for {}",
                    id, path);
        }

        final String uploadDir = multipartUploadDir(id);
        mantaClient.putDirectory(uploadDir, true);

        final String metadataPath = uploadDir + SEPARATOR + METADATA_FILE;

        final MultipartMetadata metadata = new MultipartMetadata()
                .setPath(path)
                .setObjectMetadata(mantaMetadata);

        if (httpHeaders != null) {
            metadata.setContentType(httpHeaders.getContentType());
        }

        final byte[] metadataBytes = SerializationUtils.serialize(metadata);

        mantaClient.put(metadataPath, metadataBytes);

        return new MantaMultipartUpload(id, path);
    }

    /**
     * When true indicates that multipart upload has already started.
     * @param upload multipart upload object
     * @return true if the upload has started
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public boolean isStarted(final MantaMultipartUpload upload) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return isStarted(upload.getId());
    }

    /**
     * When true indicates that multipart upload has already started.
     * @param id multipart transaction id
     * @return true if the upload has started
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public boolean isStarted(final UUID id) throws IOException {
        final String dir = multipartUploadDir(id);

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
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }

            throw e;
        }
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param contents String contents to be written in UTF-8
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final MantaMultipartUpload upload,
                                             final int partNumber,
                                             final String contents)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return putPart(upload.getId(), partNumber, contents);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param contents String contents to be written in UTF-8
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final UUID id, final int partNumber,
                                       final String contents)
            throws IOException {

        final String path = multipartPath(id, partNumber);

        final MantaObjectResponse response = mantaClient.put(path, contents);
        return new MantaMultipartUpload.Part(response);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param bytes byte array containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final MantaMultipartUpload upload,
                                             final int partNumber,
                                             final byte[] bytes)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return putPart(upload.getId(), partNumber, bytes);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param bytes byte array containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final UUID id, final int partNumber,
                                             final byte[] bytes)
            throws IOException {
        final String path = multipartPath(id, partNumber);

        final MantaObjectResponse response = mantaClient.put(path, bytes);
        return new MantaMultipartUpload.Part(response);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param file file containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final MantaMultipartUpload upload,
                                             final int partNumber,
                                             final File file)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return putPart(upload.getId(), partNumber, file);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param file file containing data of the part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final UUID id, final int partNumber,
                                             final File file)
            throws IOException {
        final String path = multipartPath(id, partNumber);

        final MantaObjectResponse response = mantaClient.put(path, file);
        return new MantaMultipartUpload.Part(response);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @param inputStream stream providing data for part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final MantaMultipartUpload upload,
                                             final int partNumber,
                                             final InputStream inputStream)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return putPart(upload.getId(), partNumber, inputStream);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param inputStream stream providing data for part to be uploaded
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part putPart(final UUID id, final int partNumber,
                                             final InputStream inputStream)
            throws IOException {
        final String path = multipartPath(id, partNumber);
        final MantaObjectResponse response = mantaClient.put(path, inputStream);

        return new MantaMultipartUpload.Part(response);
    }

    /**
     * Retrieves information about a single part of a multipart upload.
     *
     * @param upload multipart upload object
     * @param partNumber part number to identify relative location in final file
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part getPart(final MantaMultipartUpload upload,
                                             final int partNumber)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return getPart(upload.getId(), partNumber);
    }

    /**
     * Retrieves information about a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @return multipart single part object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaMultipartUpload.Part getPart(final UUID id, final int partNumber)
            throws IOException {
        final String path = multipartPath(id, partNumber);
        final MantaObjectResponse response = mantaClient.head(path);

        return new MantaMultipartUpload.Part(response);
    }

    /**
     * Lists the parts that have already been uploaded.
     *
     * @param upload multipart upload object
     * @return stream of parts identified by integer part number
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public Stream<MantaMultipartUpload.Part> listParts(final MantaMultipartUpload upload)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return listParts(upload.getId());
    }

    /**
     * Lists the parts that have already been uploaded.
     *
     * @param id multipart upload id
     * @return stream of parts identified by integer part number
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public Stream<MantaMultipartUpload.Part> listParts(final UUID id) throws IOException {
        final String dir = multipartUploadDir(id);

        return mantaClient.listObjects(dir)
                .filter(value -> !Paths.get(value.getPath())
                        .getFileName().toString().equals(METADATA_FILE))
                .map(MantaMultipartUpload.Part::new);
    }

    /**
     * Validates that there is no part missing from the sequence.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void validateThereAreNoMissingParts(final MantaMultipartUpload upload)
            throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        validateThereAreNoMissingParts(upload.getId());
    }

    /**
     * Validates that there is no part missing from the sequence.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void validateThereAreNoMissingParts(final UUID id) throws IOException {
        listParts(id)
            .sorted()
            .map(MantaMultipartUpload.Part::getPartNumber)
            .reduce(1, (memo, value) -> {
                if (!memo.equals(value)) {
                    MantaClientException e = new MantaClientException(
                            "Missing part of multipart upload");
                    e.setContextValue("missing_part", memo);
                    throw e;
                }

                return memo + 1;
            });
    }

    /**
     * Aborts a multipart transfer.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void abort(final MantaMultipartUpload upload) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        abort(upload.getId());
    }

    /**
     * Aborts a multipart transfer.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void abort(final UUID id) throws IOException {
        final String dir = multipartUploadDir(id);

        final MantaJob job = findJob(id);

        if (job.getState().equals("running")) {
            mantaClient.cancelJob(job.getId());
        }

        mantaClient.deleteRecursive(dir);
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void complete(final MantaMultipartUpload upload) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        complete(upload.getId());
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void complete(final UUID id) throws IOException {
        final String uploadDir = multipartUploadDir(id);
        final MultipartMetadata metadata = downloadMultipartMetadata(id);

        final String path = metadata.getPath();

        final StringBuilder jobExecText = new StringBuilder("mget -q ");
        try (final Stream<MantaMultipartUpload.Part> parts = listParts(id).sorted()) {
            parts.forEach(part ->
                    jobExecText.append(part.getObjectPath())
                            .append(" ")
            );
        }

        jobExecText.append("| mput -q ")
                   .append(path)
                   .append(" ");

        if (metadata.getContentType() != null) {
            jobExecText.append("-H 'Content-Type: ")
                       .append(metadata.getContentType())
                       .append("' ");
        }

        final MantaMetadata objectMetadata = metadata.getObjectMetadata();

        if (objectMetadata != null) {
            Set<Map.Entry<String, String>> entries = objectMetadata.entrySet();

            for (Map.Entry<String, String> entry : entries) {
                jobExecText.append("-H '")
                           .append(entry.getKey()).append(": ")
                           .append(entry.getValue())
                           .append("' ");
            }
        }

        final MantaJobPhase concatPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec(jobExecText.toString());

        final MantaJobPhase cleanupPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec("mrm -r " + uploadDir);

        MantaJobBuilder.Run run = mantaClient.jobBuilder()
                .newJob("append-" + id)
                .addPhase(concatPhase)
                .addPhase(cleanupPhase)
                .run();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created job for concatenating parts: {}",
                    run.getJob().getId());
        }
    }

    /**
     * Downloads the serialized metadata object from Manta and deserializes it.
     *
     * @param id multipart upload id
     * @return metadata object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    protected MantaMultipartManager.MultipartMetadata downloadMultipartMetadata(final UUID id)
            throws IOException {
        final String uploadDir = multipartUploadDir(id);
        final String metadataPath = uploadDir + SEPARATOR + METADATA_FILE;

        try (final InputStream in = mantaClient.getAsInputStream(metadataPath)){
            return SerializationUtils.deserialize(in);
        }
    }

    /**
     * Waits for a multipart upload to complete. Polling every 5 seconds.
     *
     * @param upload multipart upload object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void waitForCompletion(final MantaMultipartUpload upload) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        waitForCompletion(upload.getId());
    }

    /**
     * Waits for a multipart upload to complete. Polling every 5 seconds.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void waitForCompletion(final UUID id) throws IOException {
        waitForCompletion(id, Duration.ofSeconds(DEFAULT_SECONDS_TO_POLL),
                NUMBER_OF_TIMES_TO_POLL);
    }

    /**
     * Waits for a multipart upload to complete. Polling for set interval.
     *
     * @param upload multipart upload object
     * @param pingInterval interval to poll
     * @param timesToPoll number of times to poll Manta to check for completion
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void waitForCompletion(final MantaMultipartUpload upload,
                                  final Duration pingInterval,
                                  final int timesToPoll) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        waitForCompletion(upload.getId(), pingInterval, timesToPoll);
    }

    /**
     * Waits for a multipart upload to complete. Polling for set interval.
     *
     * @param id multipart upload id
     * @param pingInterval interval to poll
     * @param timesToPoll number of times to poll Manta to check for completion
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void waitForCompletion(final UUID id, final Duration pingInterval,
                                  final int timesToPoll) throws IOException {
        final MantaJob job = findJob(id);

        if (job == null) {
            return;
        }

        MantaJobBuilder.Run run = new MantaJobBuilder.Run(mantaClient.jobBuilder(),
                job.getId());
        run.waitUntilDone(pingInterval, timesToPoll);

        if (LOG.isDebugEnabled()) {
            LOG.debug("No longer waiting for multipart to complete."
                    + " Actual job state: {}", run.getJob().getState());
        }

        final String path = multipartUploadDir(id);
        final long waitMillis = pingInterval.toMillis();

        /* We ping the upload directory and wait for it to be deleted because
         * there is the chance for a race condition when the job attempts to
         * delete the upload directory, but isn't finished. */
        for (int i = 0; i < timesToPoll && mantaClient.existsAndIsAccessible(path); i++) {
            try {
                Thread.sleep(waitMillis);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    /**
     * Indicates if a multipart transfer has completed, cancelled or erred.
     *
     * @param upload multipart upload object
     * @return true if a multipart transfer has completed, cancelled or erred
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public boolean isComplete(final MantaMultipartUpload upload) throws IOException {
        if (upload == null) {
            throw new IllegalArgumentException("Upload must be present");
        }

        return isComplete(upload.getId());
    }

    /**
     * Indicates if a multipart transfer has completed, cancelled or erred.
     *
     * @param id multipart upload id
     * @return true if a multipart transfer has completed, cancelled or erred
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public boolean isComplete(final UUID id) throws IOException {
        final MantaJob job = findJob(id);

        if (job == null) {
            return true;
        }

        if (job.getCancelled()) {
            return true;
        }

        final String state = job.getState();

        return state.equals("done");
    }

    /**
     * Builds the full remote path for a part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @return temporary path on Manta to store part
     */
    String multipartPath(final UUID id, final int partNumber) {
        validatePartNumber(partNumber);
        final String dir = multipartUploadDir(id);
        return String.format("%s%d", dir, partNumber);
    }

    /**
     * Finds the directory in which to upload parts into.
     *
     * @param id multipart transaction id
     * @return temporary Manta directory in which to upload parts
     */
    String multipartUploadDir(final UUID id) {
        if (id == null) {
            throw new IllegalArgumentException("Transaction id must be present");
        }

        return this.resolvedMultipartUploadDirectory
                + SEPARATOR + id.toString() + SEPARATOR;
    }

    /**
     * Validates that the given part number is specified correctly.
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

    /**
     * Returns the Manta job used to concatenate multiple file parts.
     *
     * @param id multipart upload id
     * @return Manta job object
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    MantaJob findJob(final UUID id) throws IOException {
        return mantaClient.getJobsByName("append-" + id)
                .findFirst()
                .orElse(null);
    }

    /**
     * Inner class used only with the jobs-based multipart implementation for
     * storing header and metadata information.
     */
    static class MultipartMetadata implements Serializable {
        private static final long serialVersionUID = -4410867990710890357L;

        /**
         * Path to final object on Manta.
         */
        private String path;

        /**
         * Metadata of final object.
         */
        private HashMap<String, String> objectMetadata;

        /**
         * HTTP content type to write to the final object.
         */
        private String contentType;

        /**
         * Creates a new instance.
         */
        MultipartMetadata() {
        }

        String getPath() {
            return path;
        }

        /**
         * Sets the path to the final object on Manta.
         *
         * @param path remote Manta path
         * @return this instance
         */
        MultipartMetadata setPath(final String path) {
            this.path = path;
            return this;
        }

        /**
         * Gets the metadata associated with the final Manta object.
         *
         * @return new instance of {@link MantaMetadata} with data populated
         */
        MantaMetadata getObjectMetadata() {
            if (this.objectMetadata == null) {
                return null;
            }

            return new MantaMetadata(this.objectMetadata);
        }

        /**
         * Sets the metadata to be written to the final object on Manta.
         *
         * @param objectMetadata metadata to write
         * @return this instance
         */
        MultipartMetadata setObjectMetadata(final MantaMetadata objectMetadata) {
            if (objectMetadata != null) {
                this.objectMetadata = new HashMap<>(objectMetadata);
            } else {
                this.objectMetadata = null;
            }

            return this;
        }

        String getContentType() {
            return contentType;
        }

        /**
         * Sets http headers to write to the final object on Manta. Actually,
         * we only consume Content-Type for now.
         *
         * @param contentType HTTP content type to set for the object
         * @return this instance
         */
        MultipartMetadata setContentType(final String contentType) {
            this.contentType = contentType;
            return this;
        }
    }
}
