package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.commons.codec.Charsets;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;

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
public class ServerSideMultipartManager
        implements MantaMultipartManager<ServerSideMultipartUpload, MantaMultipartUploadPart> {
    /**
     * Configuration context used to get home directory.
     */
    private final ConfigContext config;

    /**
     * Reference to the Apache HTTP Client HTTP request creation class.
     */
    private final MantaConnectionFactory connectionFactory;

    /**
     * Current connection context used for maintaining state between requests.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Creates a new instance of a server-side MPU manager using the specified
     * configuration and connection builder objects.
     *
     * @param config configuration context
     * @param connectionFactory connection configuration and setup object
     * @param connectionContext connection execution object
     */
    public ServerSideMultipartManager(final ConfigContext config,
                                      final MantaConnectionFactory connectionFactory,
                                      final MantaConnectionContext connectionContext) {
        this.config = config;
        this.connectionFactory = connectionFactory;
        this.connectionContext = connectionContext;
    }

    @Override
    public Stream<ServerSideMultipartUpload> listInProgress() throws IOException {
        final String getPath = String.format("%s/uploads", config.getMantaHomeDirectory());
        final HttpGet get = connectionFactory.get(getPath);

        final int expectedStatusCode = HttpStatus.SC_OK;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() != expectedStatusCode) {
                String msg = "Unable to list multipart uploads in progress";
                MantaMultipartException e = new MantaMultipartException(msg);
                HttpHelper.annotateContextedException(e, get, response);

                throw e;
            }

            return null;
        }
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path) throws IOException {
        return initiateUpload(path, null, null);
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final MantaMetadata mantaMetadata)
            throws IOException {
        return initiateUpload(path, mantaMetadata, null);
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                               final MantaMetadata mantaMetadata,
                                               final MantaHttpHeaders httpHeaders)
            throws IOException {
        final String postPath = String.format("%s/uploads", config.getMantaHomeDirectory());
        final HttpPost post = connectionFactory.post(postPath);

        final byte[] jsonRequest = createMpuRequestBody(path, mantaMetadata, httpHeaders);
        final HttpEntity entity = new ExposedByteArrayEntity(
                jsonRequest, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        final int expectedStatusCode = HttpStatus.SC_CREATED;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() != expectedStatusCode) {
                String msg = "Unable to create multipart upload";
                MantaMultipartException e = new MantaMultipartException(msg);
                HttpHelper.annotateContextedException(e, post, response);
                e.setContextValue("objectPath", path);
                e.setContextValue("jsonRequestBody", new String(jsonRequest, Charsets.UTF_8));

                throw e;
            }

            if (response.getEntity() == null) {
                String msg = "Entity response was null";
                MantaMultipartException e = new MantaMultipartException(msg);
                HttpHelper.annotateContextedException(e, post, response);
                e.setContextValue("objectPath", path);
                e.setContextValue("jsonRequestBody", new String(jsonRequest, Charsets.UTF_8));

                throw e;
            }

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode mpu = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode idNode = mpu.get("id");
                Validate.notNull(idNode, "No multipart id returned in response");
                UUID uploadId = UUID.fromString(idNode.textValue());

                JsonNode partsDirectoryNode = mpu.get("partsDirectory");
                Validate.notNull(partsDirectoryNode, "No parts directory returned in response");
                String partsDirectory = partsDirectoryNode.textValue();

                return new ServerSideMultipartUpload(uploadId, path, partsDirectory);
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                HttpHelper.annotateContextedException(me, post, response);
                me.setContextValue("objectPath", path);
                me.setContextValue("jsonRequestBody", new String(jsonRequest, Charsets.UTF_8));

                throw me;
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                HttpHelper.annotateContextedException(me, post, response);
                me.setContextValue("objectPath", path);
                me.setContextValue("jsonRequestBody", new String(jsonRequest, Charsets.UTF_8));

                throw me;
            }
        }
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final String contents)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final byte[] bytes)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final File file) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                               final int partNumber,
                                               final InputStream inputStream)
            throws IOException {
        return null;
    }

    @Override
    public MantaMultipartUploadPart getPart(final ServerSideMultipartUpload upload,
                                            final int partNumber) throws IOException {
        return null;
    }

    @Override
    public MantaMultipartStatus getStatus(final ServerSideMultipartUpload upload)
            throws IOException {
        return null;
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(final ServerSideMultipartUpload upload)
            throws IOException {
        return null;
    }

    @Override
    public void validateThatThereAreSequentialPartNumbers(final ServerSideMultipartUpload upload)
            throws IOException, MantaMultipartException {

    }

    @Override
    public void abort(final ServerSideMultipartUpload upload) throws IOException {

    }

    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {

    }

    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {

    }

    @Override
    public <R> R waitForCompletion(final ServerSideMultipartUpload upload,
                                   final Function<UUID, R> executeWhenTimesToPollExceeded)
            throws IOException {
        return null;
    }

    @Override
    public <R> R waitForCompletion(final ServerSideMultipartUpload upload,
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
