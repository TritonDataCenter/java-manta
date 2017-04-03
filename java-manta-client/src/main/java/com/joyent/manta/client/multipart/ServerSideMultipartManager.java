/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.multipart;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObjectMapper;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaMultipartException;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.joyent.manta.client.MantaClient.SEPARATOR;

/**
 * Class providing a server-side natively supported implementation
 * of multipart uploads.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class ServerSideMultipartManager extends AbstractMultipartManager
        <ServerSideMultipartUpload, MantaMultipartUploadPart> {
    /**
     * Logger instance.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerSideMultipartManager.class);

    /**
     * Server-side MPU only supports 10k parts.
     */
    private static final int MAX_PARTS = 10_000;

    /**
     * Minimum size of a part in bytes.
     */
    private static final int MIN_PART_SIZE = 5_242_880; // 5 mebibytes

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
     * Reference to an open client.
     */
    private final MantaClient mantaClient;

    /**
     * Reference to the collection of all of the {@link AutoCloseable} objects
     * that will need to be closed when MantaClient is closed. This reference
     * should point to the value set on the MantaClient field.
     */
    private final Set<AutoCloseable> danglingStreams;

    /**
     * Creates a new instance of a server-side MPU manager using the specified
     * configuration and connection builder objects.
     *
     * @param mantaClient open Manta client instance
     */
    public ServerSideMultipartManager(final MantaClient mantaClient) {
        super();
        Validate.isTrue(!mantaClient.isClosed(),
                "Manta client must not be closed");
        this.config = mantaClient.getContext();
        this.mantaClient = mantaClient;

        /* These fields are not exposed in the MantaClient because they expose
         * implementation details about the HTTP Client library that is shaded.
         * Using reflection here, shouldn't be a big performance hit because
         * you would typically create one manager instance and reuse it for
         * multiple uploads. */
        this.connectionContext = readFieldFromMantaClient(
                "connectionContext", mantaClient, MantaConnectionContext.class);
        this.connectionFactory = readFieldFromMantaClient(
                "connectionFactory", mantaClient, MantaConnectionFactory.class);
        @SuppressWarnings("unchecked")
        Set<AutoCloseable> dangling = (Set<AutoCloseable>)readFieldFromMantaClient(
                        "danglingStreams", mantaClient, Set.class);
        this.danglingStreams = dangling;
    }

    /**
     * Creates a new instance of a server-side MPU manager using the specified
     * configuration and connection builder objects.
     *
     * @param config configuration context
     * @param connectionFactory connection configuration and setup object
     * @param connectionContext connection execution object
     * @param mantaClient open Manta client instance
     */
    ServerSideMultipartManager(final ConfigContext config,
                               final MantaConnectionFactory connectionFactory,
                               final MantaConnectionContext connectionContext,
                               final MantaClient mantaClient) {
        super();

        Validate.isTrue(!mantaClient.isClosed(),
                "MantaClient must not be closed");

        this.config = config;
        this.connectionFactory = connectionFactory;
        this.connectionContext = connectionContext;
        this.mantaClient = mantaClient;

        @SuppressWarnings("unchecked")
        Set<AutoCloseable> dangling = (Set<AutoCloseable>)readFieldFromMantaClient(
                "danglingStreams", mantaClient, Set.class);
        this.danglingStreams = dangling;
    }

    @Override
    public int getMaxParts() {
        return MAX_PARTS;
    }

    @Override
    public int getMinimumPartSize() {
        return MIN_PART_SIZE;
    }


    @Override
    public Stream<MantaMultipartUpload> listInProgress() throws IOException {
        final String uploadsPath = uploadsPath();

        Stream<MantaMultipartUpload> stream = mantaClient.listObjects(uploadsPath)
                .map(mantaObject -> {
            try {
                return mantaClient.listObjects(mantaObject.getPath()).map(item -> {
                    final String objectName = FilenameUtils.getName(item.getPath());
                    final UUID id = UUID.fromString(objectName);

                    // We don't know the final object name. The server will implement
                    // this as a feature in the future.

                    return new ServerSideMultipartUpload(id, null, uuidPrefixedPath(id));
                }).collect(Collectors.toSet());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).flatMap(Collection::stream);

        danglingStreams.add(stream);

        return stream;
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path) throws IOException {
        return initiateUpload(path, new MantaMetadata());
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final MantaMetadata mantaMetadata)
            throws IOException {
        return initiateUpload(path, mantaMetadata, new MantaHttpHeaders());
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final MantaMetadata mantaMetadata,
                                                    final MantaHttpHeaders httpHeaders)
            throws IOException {
        return initiateUpload(path, null, mantaMetadata, httpHeaders);
    }

    @Override
    public ServerSideMultipartUpload initiateUpload(final String path,
                                                    final Long contentLength,
                                                    final MantaMetadata mantaMetadata,
                                                    final MantaHttpHeaders httpHeaders)
            throws IOException {
        Validate.notNull(path, "Path to object must not be null");
        Validate.notBlank(path, "Path to object must not be blank");

        final MantaMetadata metadata;

        if (mantaMetadata == null) {
            metadata = new MantaMetadata();
        } else {
            metadata = mantaMetadata;
        }

        final MantaHttpHeaders headers;

        if (httpHeaders == null) {
            headers = new MantaHttpHeaders();
        } else {
            headers = httpHeaders;
        }

        /* We explicitly set the content-length header if it is passed as a method parameter
         * so that the server will validate the size of the upload when it is committed. */
        if (contentLength != null && headers.getContentLength() == null) {
            headers.setContentLength(contentLength);
        }

        final String postPath = uploadsPath();
        final HttpPost post = connectionFactory.post(postPath);

        final byte[] jsonRequest = createMpuRequestBody(path, metadata, headers);
        final HttpEntity entity = new ExposedByteArrayEntity(
                jsonRequest, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        final int expectedStatusCode = HttpStatus.SC_CREATED;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to create multipart upload", post,
                    response, path, jsonRequest);
            validateEntityIsPresent(post, response, path, jsonRequest);

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
                annotateException(me, post, response, path, jsonRequest);
                throw me;
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, post, response, path, jsonRequest);
                throw me;
            }
        }
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
    @Override
    MantaMultipartUploadPart uploadPart(final ServerSideMultipartUpload upload,
                                        final int partNumber,
                                        final HttpEntity entity)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        validatePartNumber(partNumber);

        /* Manta starts counting parts at 0 - the SDK starts counting at 1.
         * This is for two reasons. 1) It provides better compatibility with libraries
         * that also have to interact with S3. 2) It provides backwards compatibility
         * with the jobs based multipart implementation.
         */
        final int adjustedPartNumber = partNumber - 1;

        final String putPath = upload.getPartsDirectory() + SEPARATOR + adjustedPartNumber;
        final HttpPut put = connectionFactory.put(putPath);
        put.setEntity(entity);

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(put)) {
            Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);
            final String etag;

            if (etagHeader != null) {
                etag = etagHeader.getValue();
            } else {
                etag = null;
            }

            return new MantaMultipartUploadPart(partNumber, upload.getPath(), etag);
        }
    }

    @Override
    public MantaMultipartUploadPart getPart(final ServerSideMultipartUpload upload,
                                            final int partNumber) throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");
        validatePartNumber(partNumber);

        final int adjustedPartNumber = partNumber - 1;

        final String getPath = upload.getPartsDirectory() + SEPARATOR + "state";
        final HttpGet get = connectionFactory.get(getPath);

        final String objectPath;

        final int expectedStatusCode = HttpStatus.SC_OK;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(get)) {
            StatusLine statusLine = response.getStatusLine();
            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload", get,
                    response, null, null);
            validateEntityIsPresent(get, response, null, null);

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode objectNode = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode objectPathNode = objectNode.get("objectPath");
                Validate.notNull(objectPathNode, "Unable to read object path from response");
                objectPath = objectPathNode.textValue();
                Validate.notBlank(objectPath, "Object path field was blank in response");
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }

        final String headPath = upload.getPartsDirectory() + SEPARATOR + adjustedPartNumber;
        final HttpHead head = connectionFactory.head(headPath);

        final String etag;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(head)) {
            StatusLine statusLine = response.getStatusLine();

            if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return null;
            }

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload part", get,
                    response, null, null);

            try {
                final Header etagHeader = response.getFirstHeader(HttpHeaders.ETAG);
                Validate.notNull(etagHeader, "ETag header was not returned");
                etag = etagHeader.getValue();
                Validate.notBlank(etag, "ETag is blank");
            } catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected header was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }

        return new MantaMultipartUploadPart(partNumber, objectPath, etag);
    }

    @Override
    public MantaMultipartStatus getStatus(final ServerSideMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String partsDirectory;

        if (upload.getPartsDirectory() == null) {
            partsDirectory = uuidPrefixedPath(upload.getId());
        } else {
            partsDirectory = upload.getPartsDirectory();
        }

        final String getPath = partsDirectory + SEPARATOR + "state";
        final HttpGet get = connectionFactory.get(getPath);

        final int expectedStatusCode = HttpStatus.SC_OK;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(get)) {
            StatusLine statusLine = response.getStatusLine();

            if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return MantaMultipartStatus.UNKNOWN;
            }

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to get status for multipart upload", get,
                    response, null, null);
            validateEntityIsPresent(get, response, null, null);

            try (InputStream in = response.getEntity().getContent()) {
                ObjectNode objectNode = MantaObjectMapper.INSTANCE.readValue(in, ObjectNode.class);

                JsonNode stateNode = objectNode.get("state");
                Validate.notNull(stateNode, "Unable to get state from response");
                String state = stateNode.textValue();
                Validate.notBlank(state, "State field was blank in response");

                if (state.equalsIgnoreCase("created")) {
                    return MantaMultipartStatus.CREATED;
                }

                if (state.equalsIgnoreCase("finalizing")) {
                    JsonNode typeNode = objectNode.get("type");
                    Validate.notNull(typeNode, "Unable to get type from response");
                    String type = typeNode.textValue();
                    Validate.notBlank(type, "Type field was blank in response");

                    if (type.equalsIgnoreCase("commit")) {
                        return MantaMultipartStatus.COMMITTING;
                    }
                    if (type.equalsIgnoreCase("abort")) {
                        return MantaMultipartStatus.ABORTING;
                    }
                }

                return MantaMultipartStatus.UNKNOWN;
            } catch (JsonParseException e) {
                String msg = "Response body was not JSON";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }  catch (NullPointerException | IllegalArgumentException e) {
                String msg = "Expected response field was missing or malformed";
                MantaMultipartException me = new MantaMultipartException(msg, e);
                annotateException(me, get, response, null, null);
                throw me;
            }
        }
    }

    @Override
    public Stream<MantaMultipartUploadPart> listParts(final ServerSideMultipartUpload upload)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String partsDirectory = upload.getPartsDirectory();

        Stream<MantaMultipartUploadPart> stream = mantaClient.listObjects(partsDirectory)
                .map(mantaObject -> {
            final String item = FilenameUtils.getName(mantaObject.getPath());
            final int adjustedPartNumber = Integer.parseInt(item) + 1;

            final String etag = mantaObject.getEtag();

            return new MantaMultipartUploadPart(adjustedPartNumber, upload.getPath(), etag);
        });

        danglingStreams.add(stream);

        return stream;
    }

    @Override
    public void abort(final ServerSideMultipartUpload upload) throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String postPath = upload.getPartsDirectory() + SEPARATOR + "abort";
        final HttpPost post = connectionFactory.post(postPath);

        final int expectedStatusCode = HttpStatus.SC_NO_CONTENT;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();
            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to abort multipart upload", post,
                    response, null, null);
            LOGGER.info("Aborted multipart upload [id={}]", upload.getId());
        }
    }

    /**
     * Completes a multipart transfer by assembling the parts on Manta.
     * This is a synchronous operation.
     *
     * @param upload multipart upload object
     * @param parts iterable of multipart part objects
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Iterable<? extends MantaMultipartUploadTuple> parts)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final Stream<? extends MantaMultipartUploadTuple> partsStream =
                StreamSupport.stream(parts.spliterator(), false);

        complete(upload, partsStream);
    }

    /**
     * <p>Completes a multipart transfer by assembling the parts on Manta as an
     * synchronous operation.</p>
     *
     * <p>Note: this performs a terminal operation on the partsStream and
     * thereby will close the stream.</p>
     *
     * @param upload multipart upload object
     * @param partsStream stream of multipart part objects
     * @throws IOException thrown if there is a problem connecting to Manta
     */
    @Override
    public void complete(final ServerSideMultipartUpload upload,
                         final Stream<? extends MantaMultipartUploadTuple> partsStream)
            throws IOException {
        Validate.notNull(upload, "Upload state object must not be null");

        final String path = upload.getPath();
        final String postPath = upload.getPartsDirectory();
        final HttpPost post = connectionFactory.post(postPath + "/commit");

        final byte[] jsonRequest;
        final int numParts;

        try {
            ImmutablePair<byte[], Integer> pair = createCommitRequestBody(partsStream);
            jsonRequest = pair.getLeft();
            numParts = pair.getRight();
        } catch (NullPointerException | IllegalArgumentException e) {
            String msg = "Expected response field was missing or malformed";
            MantaMultipartException me = new MantaMultipartException(msg, e);
            annotateException(me, post, null, path, null);
            throw me;
        }

        final HttpEntity entity = new ExposedByteArrayEntity(
                jsonRequest, ContentType.APPLICATION_JSON);
        post.setEntity(entity);

        final int expectedStatusCode = HttpStatus.SC_CREATED;

        try (CloseableHttpResponse response = connectionContext.getHttpClient().execute(post)) {
            StatusLine statusLine = response.getStatusLine();

            validateStatusCode(expectedStatusCode, statusLine.getStatusCode(),
                    "Unable to create multipart upload", post,
                    response, path, jsonRequest);

            Header location = response.getFirstHeader(HttpHeaders.LOCATION);

            if (location != null && LOGGER.isInfoEnabled()) {
                LOGGER.info("Multipart upload [{}] for file [{}] from [{}] parts has completed",
                            upload.getId(), location.getValue(), numParts);
            }
        }
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
    static byte[] createMpuRequestBody(final String objectPath,
                                       final MantaMetadata mantaMetadata,
                                       final MantaHttpHeaders headers) {
        Validate.notNull(objectPath, "Path to Manta object must not be null");

        CreateMPURequestBody requestBody = new CreateMPURequestBody(
                objectPath, mantaMetadata, headers);

        try {
            return MantaObjectMapper.INSTANCE.writeValueAsBytes(requestBody);
        } catch (IOException e) {
            String msg = "Error serializing JSON for create MPU request body";
            throw new MantaMultipartException(msg, e);
        }
    }

    /**
     * Creates the JSON request body used to commit all of the parts of a multipart
     * upload request.
     *
     * @param parts stream of tuples - this is a terminal operation that will close the stream
     * @return byte array containing JSON data
     */
    static ImmutablePair<byte[], Integer>
        createCommitRequestBody(final Stream<? extends MantaMultipartUploadTuple> parts) {
        final JsonNodeFactory nodeFactory = MantaObjectMapper.NODE_FACTORY_INSTANCE;
        final ObjectNode objectNode = new ObjectNode(nodeFactory);

        final ArrayNode partsArrayNode = new ArrayNode(nodeFactory);
        objectNode.set("parts", partsArrayNode);

        try (Stream<? extends MantaMultipartUploadTuple> sorted = parts.sorted()) {
            sorted.forEach(tuple -> partsArrayNode.add(tuple.getEtag()));
        }

        Validate.isTrue(partsArrayNode.size() > 0,
                "Can't commit multipart upload with no parts");

        try {
            return ImmutablePair.of(MantaObjectMapper.INSTANCE.writeValueAsBytes(objectNode), partsArrayNode.size());
        } catch (IOException e) {
            String msg = "Error serializing JSON for commit MPU request body";
            throw new MantaMultipartException(msg, e);
        }
    }

    /**
     * Validates that the status code received is the expected status code.
     *
     * @param expectedCode expected HTTP status code
     * @param actualCode actual HTTP status code
     * @param errorMessage error message to attach to exception
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     * @throws MantaMultipartException thrown when the status codes do not match
     */
    private void validateStatusCode(final int expectedCode,
                                    final int actualCode,
                                    final String errorMessage,
                                    final HttpRequest request,
                                    final HttpResponse response,
                                    final String objectPath,
                                    final byte[] requestBody) {
        if (actualCode != expectedCode) {
            final String path;

            if (objectPath == null && request != null) {
                path = request.getRequestLine().getUri();
            } else {
                path = objectPath;
            }
            // We create the exception below because it will parse the JSON error codes
            MantaClientHttpResponseException mchre =
                    new MantaClientHttpResponseException(request, response, path);
            // We chain it to this exception so that it obeys the contract
            MantaMultipartException e = new MantaMultipartException(errorMessage, mchre);
            annotateException(e, request, response, objectPath, requestBody);
            throw e;
        }
    }

    /**
     * @return path to the server-side multipart uploads directory
     */
    private String uploadsPath() {
        return config.getMantaHomeDirectory() + SEPARATOR + "uploads";
    }

    /**
     * Creates a <code>$home/uploads/directory/directory</code> path with
     * the first letter of a uuid being the first directory and the uuid itself
     * being the second directory.
     *
     * @param uuid uuid to create path from
     * @return uuid prefixed directories
     */
    String uuidPrefixedPath(final UUID uuid) {
        Validate.notNull(uuid, "UUID must not be null");

        final String uuidString = uuid.toString();

        return uploadsPath() + SEPARATOR
                + uuidString.substring(0, 1)
                + SEPARATOR + uuidString;
    }

    /**
     * Validates that the response has a valid entity.
     *
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     * @throws MantaMultipartException thrown when the entity is null
     * @throws MantaIOException thrown when unable to get entity's InputStream
     */
    private void validateEntityIsPresent(final HttpRequest request,
                                         final HttpResponse response,
                                         final String objectPath,
                                         final byte[] requestBody)
            throws MantaIOException {
        if (response.getEntity() == null) {
            String msg = "Entity response was null";
            MantaMultipartException e = new MantaMultipartException(msg);
            annotateException(e, request, response, objectPath, requestBody);
            throw e;
        }

        try {
            if (response.getEntity().getContent() == null) {
                String msg = "Entity content InputStream was null";
                MantaMultipartException e = new MantaMultipartException(msg);
                annotateException(e, request, response, objectPath, requestBody);
                throw e;
            }
        } catch (IOException e) {
            String msg = "Unable to get an InputStream from the HTTP entity";
            MantaIOException mioe = new MantaIOException(msg, e);
            annotateException(mioe, request, response, objectPath, requestBody);
            throw mioe;
        }
    }

    /**
     * Appends context attributes for the HTTP request and HTTP response objects
     * to a {@link ExceptionContext} instance using values relevant to this
     * class.
     *
     * @param exception exception to append to
     * @param request HTTP request object
     * @param response HTTP response object
     * @param objectPath path to the object being operated on
     * @param requestBody contents of request body as byte array
     */
    private void annotateException(final ExceptionContext exception,
                                   final HttpRequest request,
                                   final HttpResponse response,
                                   final String objectPath,
                                   final byte[] requestBody) {
        HttpHelper.annotateContextedException(exception, request, response);

        if (objectPath != null) {
            exception.setContextValue("objectPath", objectPath);
        }

        if (requestBody != null) {
            exception.setContextValue("requestBody", new String(requestBody,
                    StandardCharsets.UTF_8));
        }
    }
}
