package com.joyent.manta.client;

import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaIOException;
import org.apache.http.HttpStatus;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Class providing an API to multipart uploads to Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaMultipart {
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
     * Creates a new instance backed by the specified {@link MantaClient}.
     * @param mantaClient Manta client instance to use to communicate with server
     */
    public MantaMultipart(final MantaClient mantaClient) {
        if (mantaClient == null) {
            throw new IllegalArgumentException("Manta client must be present");
        }
        this.mantaClient = mantaClient;
    }

    /**
     * Initializes a new multipart upload for an object.
     * @param path remote path of Manta object to be uploaded
     * @return unique id for the multipart upload
     * @throws IOException thrown when there are network issues
     */
    public UUID initiateUpload(final String path) throws IOException {
        final UUID id = UUID.randomUUID();
        final String uploadDir = multipartUploadDir(id);
        mantaClient.putDirectory(uploadDir, true);

        final String pathData = uploadDir + SEPARATOR + "path";
        mantaClient.put(pathData, path);

        return id;
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
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param contents String contents to be written in UTF-8
     * @return server HTTP response
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaObjectResponse putPart(final UUID id, final int partNumber,
                                       final String contents)
            throws IOException {

        final String path = multipartPath(id, partNumber);

        return mantaClient.put(path, contents);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param bytes byte array containing data of the part to be uploaded
     * @return server HTTP response
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaObjectResponse putPart(final UUID id, final int partNumber,
                                       final byte[] bytes)
            throws IOException {
        final String path = multipartPath(id, partNumber);

        return mantaClient.put(path, bytes);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param file file containing data of the part to be uploaded
     * @return server HTTP response
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaObjectResponse putPart(final UUID id, final int partNumber,
                                       final File file)
            throws IOException {
        final String path = multipartPath(id, partNumber);

        return mantaClient.put(path, file);
    }

    /**
     * Uploads a single part of a multipart upload.
     *
     * @param id multipart upload id
     * @param partNumber part number to identify relative location in final file
     * @param inputStream stream providing data for part to be uploaded
     * @return server HTTP response
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public MantaObjectResponse putPart(final UUID id, final int partNumber,
                                       final InputStream inputStream)
            throws IOException {
        final String path = multipartPath(id, partNumber);

        return mantaClient.put(path, inputStream);
    }

    /**
     * Lists the parts that have already been uploaded.
     *
     * @param id multipart upload id
     * @return stream of parts identified by integer part number
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public Stream<Integer> listParts(final UUID id) throws IOException {
        final String dir = multipartUploadDir(id);

        return mantaClient.listObjects(dir)
                .map(mantaObject -> Paths.get(mantaObject.getPath())
                        .getFileName().toString())
                .filter(value -> !value.equals("path"))
                .map(Integer::parseInt);
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
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void abort(final UUID id) throws IOException {
        final String dir = multipartUploadDir(id);

        MantaJob job = mantaClient.getJobsByName("append-" + id)
                .findFirst()
                .orElse(null);

        if (job.getState().equals("running")) {
            mantaClient.cancelJob(job.getId());
        }

        mantaClient.deleteRecursive(dir);
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     *
     * @param id multipart upload id
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void complete(final UUID id) throws IOException {
        final String uploadDir = multipartUploadDir(id);
        final String pathData = uploadDir + SEPARATOR + "path";
        final String path = mantaClient.getAsString(pathData);

        final Stream<Integer> parts = listParts(id)
                .sorted();

        final StringBuilder jobExecText = new StringBuilder("mget -q ");

        parts.forEach(part ->
                jobExecText.append(uploadDir)
                           .append(SEPARATOR)
                           .append(part)
                           .append(" ")
        );

        jobExecText.append("| mput -q ")
                   .append(path);

        final MantaJobPhase concatPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec(jobExecText.toString());

        final MantaJobPhase cleanupPhase = new MantaJobPhase()
                .setType("reduce")
                .setExec("mrm -r " + uploadDir);

        mantaClient.jobBuilder()
                .newJob("append-" + id)
                .addPhase(concatPhase)
                .addPhase(cleanupPhase)
                .run();
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
     * @param id multipart upload id
     * @param pingInterval interval to poll
     * @param timesToPoll number of times to poll Manta to check for completion
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public void waitForCompletion(final UUID id, final Duration pingInterval,
                                  final int timesToPoll) throws IOException {
        MantaJob job = mantaClient.getJobsByName("append-" + id)
                .findFirst()
                .orElse(null);

        if (job == null) {
            return;
        }

        MantaJobBuilder.Run run = new MantaJobBuilder.Run(mantaClient.jobBuilder(),
                job.getId());
        run.waitUntilDone(pingInterval, timesToPoll);
    }

    /**
     * Indicates if a multipart transfer has completed, cancelled or erred.
     *
     * @param id multipart upload id
     * @return true if a multipart transfer has completed, cancelled or erred
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    public boolean isComplete(final UUID id) throws IOException {
        MantaJob job = mantaClient.getJobsByName("append-" + id)
                .findFirst()
                .orElse(null);

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

        final ConfigContext config = mantaClient.getContext();

        return config.getMantaHomeDirectory()
                + SEPARATOR + MULTIPART_DIRECTORY
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
}
