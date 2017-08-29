/*
 * Copyright (c) 2013-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.http.signature.Signer;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.manta.client.crypto.ExternalSecurityProviderLoader;
import com.joyent.manta.client.jobs.MantaJob;
import com.joyent.manta.client.jobs.MantaJobBuilder;
import com.joyent.manta.client.jobs.MantaJobError;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.config.KeyPairFactory;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaJobException;
import com.joyent.manta.exception.OnCloseAggregateException;
import com.joyent.manta.http.ContentTypeLookup;
import com.joyent.manta.http.EncryptionHttpHelper;
import com.joyent.manta.http.HttpHelper;
import com.joyent.manta.http.MantaApacheHttpClientContext;
import com.joyent.manta.http.MantaConnectionContext;
import com.joyent.manta.http.MantaConnectionFactory;
import com.joyent.manta.http.MantaContentTypes;
import com.joyent.manta.http.MantaHttpHeaders;
import com.joyent.manta.http.StandardHttpHelper;
import com.joyent.manta.http.entity.ExposedByteArrayEntity;
import com.joyent.manta.http.entity.ExposedStringEntity;
import com.joyent.manta.util.ConcurrentWeakIdentityHashMap;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.joyent.manta.exception.MantaErrorCode.DIRECTORY_NOT_EMPTY_ERROR;
import static com.joyent.manta.util.MantaUtils.formatPath;

/**
 * Manta client object that allows for doing CRUD operations against the Manta HTTP
 * API. Using this client you can retrieve implementations of {@link MantaObject}.
 *
 * @author <a href="https://github.com/yunong">Yunong Xiao</a>
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaClient implements AutoCloseable {
    /**
     * Directory separator used in Manta.
     */
    public static final String SEPARATOR = "/";

    /**
     * The static logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaClient.class);

    /**
     * HTTP metadata headers that violate the Manta API contract.
     */
    private static final String[] ILLEGAL_METADATA_HEADERS = new String[]{
            HttpHeaders.CONTENT_LENGTH, "Content-MD5", "Durability-Level"
    };

    /**
     * Maximum number of results to return for a directory listing.
     */
    private static final int MAX_RESULTS = 1024;

    /**
     * Flag indicating if the client instance has been closed.
     */
    private volatile boolean closed = false;

    /**
     * A string representation of the manta service endpoint URL.
     */
    private final String url;

    /**
     * The instance of the http helper class used to simplify creating requests.
     */
    private final HttpHelper httpHelper;

    /**
     * The home directory of the account.
     */
    private final String home;

    /**
     * Library configuration context reference.
     */
    private final ConfigContext config;

    /**
     * Collection of all of the {@link AutoCloseable} objects that will need to be
     * closed when MantaClient is closed.
     */
    private final Set<AutoCloseable> danglingStreams
            = (Collections.newSetFromMap(new ConcurrentWeakIdentityHashMap<>()));

    /**
     * Instance used to generate Manta signed URIs.
     */
    private final UriSigner uriSigner;

    /**
     * Object containing saved context used between requests to the Manta client.
     */
    private final MantaConnectionContext connectionContext;

    /**
     * Connection factory instance used for building requests to Manta.
     */
    private final MantaConnectionFactory connectionFactory;

    /* We preform some sanity checks against the JVM in order to determine if
     * we can actually run on the platform. */
    static {
        LOG.debug("Preferred Security Provider: {}",
                ExternalSecurityProviderLoader.getPreferredProvider());
    }

    /**
     * Creates a new instance of a Manta client.
     *
     * @param config The configuration context that provides all of the configuration values.
     */
    public MantaClient(final ConfigContext config) {
        dumpConfig(config);

        final String mantaURL = config.getMantaURL();
        final String account = config.getMantaUser();

        ConfigContext.validate(config);

        this.url = mantaURL;
        this.config = config;
        this.home = ConfigContext.deriveHomeDirectoryFromUser(account);

        final KeyPairFactory keyPairFactory = new KeyPairFactory(config);
        final KeyPair keyPair = keyPairFactory.createKeyPair();

        final Signer.Builder builder = new Signer.Builder(keyPair);
        if (ObjectUtils.firstNonNull(
                config.disableNativeSignatures(),
                DefaultsConfigContext.DEFAULT_DISABLE_NATIVE_SIGNATURES)) {
            builder.providerCode("stdlib");
        }
        final ThreadLocalSigner signer = new ThreadLocalSigner(builder);

        this.connectionFactory = new MantaConnectionFactory(config, keyPair, signer);
        this.connectionContext = new MantaApacheHttpClientContext(this.connectionFactory);

        if (BooleanUtils.isTrue(config.isClientEncryptionEnabled())) {
            this.httpHelper = new EncryptionHttpHelper(connectionContext, connectionFactory,
                    config);
        } else {
            this.httpHelper = new StandardHttpHelper(connectionContext, connectionFactory,
                    config);
        }

        this.uriSigner = new UriSigner(this.config, keyPair, signer);
    }

    /**
     * Creates a new instance of the Manta client with the specified connection factory.
     * This method is typically used by unit tests to inject connection factory
     * customizations so we can simulate different network events.
     *
     * @param config The configuration context that provides all of the configuration values
     * @param keyPair cryptographic signing key pair used for HTTP signatures
     * @param connectionFactory connection factory instance used to tune connection settings
     */
    MantaClient(final ConfigContext config,
                final KeyPair keyPair,
                final MantaConnectionFactory connectionFactory,
                final ThreadLocalSigner signer) {
        dumpConfig(config);

        final String mantaURL = config.getMantaURL();
        final String account = config.getMantaUser();

        ConfigContext.validate(config);

        this.url = mantaURL;
        this.config = config;
        this.home = ConfigContext.deriveHomeDirectoryFromUser(account);

        this.connectionFactory = connectionFactory;
        this.connectionContext = new MantaApacheHttpClientContext(this.connectionFactory);

        if (BooleanUtils.isTrue(config.isClientEncryptionEnabled())) {
            this.httpHelper = new EncryptionHttpHelper(connectionContext, connectionFactory,
                    config);
        } else {
            this.httpHelper = new StandardHttpHelper(connectionContext, connectionFactory,
                    config);
        }

        this.uriSigner = new UriSigner(this.config, keyPair, signer);
    }

    /**
     * Dumps the configuration that is used to load a {@link MantaClient} if
     * the Java system property manta.dumpConfig is set.
     *
     * @param context Configuration context object to dump
     */
    private static void dumpConfig(final ConfigContext context) {
        if (context == null) {
            System.out.println("========================================");
            System.out.println("Configuration Context was null");
            System.out.println("========================================");
            return;
        }

        String dumpConfigVal = System.getProperty("manta.dumpConfig");
        if (dumpConfigVal != null && MantaUtils.parseBooleanOrNull(dumpConfigVal)) {
            System.out.println("========================================");
            System.out.println(ConfigContext.toString(context));
            System.out.println("========================================");
        }
    }

    /**
     * Deletes an object from Manta.
     *
     * @param rawPath The fully qualified path of the Manta object.
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If an HTTP status code {@literal > 300} is returned.
     */
    public void delete(final String rawPath) throws IOException {
        String path = formatPath(rawPath);
        LOG.debug("DELETE {}", path);

        httpHelper.httpDelete(path);
    }

    /**
     * Recursively deletes an object in Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void deleteRecursive(final String path) throws IOException {
        LOG.debug("DELETE {} [recursive]", path);

        try {
            this.delete(path);
            LOG.debug("Finished deleting path {}", path);
            return;
        } catch (MantaClientHttpResponseException e) {
            /* In the case of the directory not being empty, we let
             * that error case go uncaught and pass through
             * because it is in fact a directory with files. */
            if (!e.getServerCode().equals(DIRECTORY_NOT_EMPTY_ERROR)) {
                throw e;
            }
        }

        try (Stream<MantaObject> objects = this.listObjects(path)) {
            objects.forEach(obj -> {
                try {
                    if (obj.isDirectory()) {
                        this.deleteRecursive(obj.getPath());
                    } else {
                        this.delete(obj.getPath());
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        /* Once we have deleted all of the sub objects of this directory, let's
         * make sure that all of the sub objects were in fact actually deleted -
         * you know because distributed systems...
         */
        boolean pathExists = existsAndIsAccessible(path);

        if (!pathExists) {
            LOG.debug("Path {} doesn't exist. Finished.", path);
            return;
        }

        try {
            this.delete(path);
        } catch (MantaClientHttpResponseException e) {
            // Directory wasn't empty
            if (e.getServerCode().equals(DIRECTORY_NOT_EMPTY_ERROR)) {
                try {
                    final int waitTime = 400;
                    Thread.sleep(waitTime);
                    LOG.warn("First attempt to delete directory failed, retrying");
                    // Re-attempt to delete the directory
                    this.deleteRecursive(path);
                } catch (InterruptedException ie) {
                    // We don't need to do anything, just exit
                }
            } else {
                throw e;
            }
        }

        LOG.debug("Finished deleting path {}", path);
    }

    /**
     * Get the metadata for a Manta object. The difference with this method vs head() is
     * that the request being made against the Manta API is done via a GET.
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectResponse}.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse get(final String rawPath) throws IOException {
        String path = formatPath(rawPath);
        final HttpResponse response = httpHelper.httpGet(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders(response.getAllHeaders());
        return new MantaObjectResponse(path, headers);
    }

    /**
     * <p>Get a Manta object's data as an {@link InputStream}. This method allows you to
     * stream data from the Manta storage service in a memory efficient manner to your
     * application.</p>
     *
     * <p><strong>It is your responsibility to close this stream. Otherwise, you
     * may end up with resource leaks.</strong></p>
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param requestHeaders optional HTTP headers to include when getting an object
     * @return {@link InputStream} that extends {@link MantaObjectResponse}.
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaObjectInputStream getAsInputStream(final String rawPath,
                                                   final MantaHttpHeaders requestHeaders)
            throws IOException {
        final String path = formatPath(rawPath);
        final HttpGet get = connectionFactory.get(path);

        MantaObjectInputStream stream = httpHelper.httpRequestAsInputStream(get,
                requestHeaders);

        danglingStreams.add(stream);

        return stream;
    }

    /**
     * @see #getAsInputStream(String, MantaHttpHeaders)
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param requestHeaders optional HTTP headers to include when getting an object
     * @param startPosition see {@link MantaHttpHeaders#setByteRange(Long, Long)}
     * @param endPosition see {@link MantaHttpHeaders#setByteRange(Long, Long)}
     * @return {@link InputStream} that extends {@link MantaObjectResponse}.
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaObjectInputStream getAsInputStream(final String rawPath,
                                                   final MantaHttpHeaders requestHeaders,
                                                   final Long startPosition,
                                                   final Long endPosition) throws IOException {
        if (requestHeaders.getRange() != null) {
            throw new IllegalArgumentException("Ambiguous request, requestHeaders already has a Range");
        }
        requestHeaders.setByteRange(startPosition, endPosition);
        return getAsInputStream(rawPath, requestHeaders);
    }

    /**
     * Get a Manta object's data as an {@link InputStream}. This method allows you to
     * stream data from the Manta storage service in a memory efficient manner to your
     * application.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return {@link InputStream} that extends {@link MantaObjectResponse}.
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaObjectInputStream getAsInputStream(final String path) throws IOException {
        return getAsInputStream(path, null);
    }

    /**
     * Get a Manta object's data as a {@link String} using the JVM's default encoding.
     * This method is not memory efficient, by loading the data into a String you are
     * loading all of the Object's data into memory.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return String containing the entire Manta object
     * @throws IOException when there is a problem getting the object over the network
     */
    public String getAsString(final String path) throws IOException {
        try (InputStream is = getAsInputStream(path)) {
            return IOUtils.toString(is, Charset.defaultCharset());
        }
    }

    /**
     * Get a Manta object's data as a {@link String} using the specified encoding.
     * This method is not memory efficient, by loading the data into a String you are
     * loading all of the Object's data into memory.
     *
     * @param path        The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param charsetName The encoding type used to convert bytes from the
     *                    stream into characters to be scanned
     * @return String containing the entire Manta object
     * @throws IOException when there is a problem getting the object over the network
     */
    public String getAsString(final String path, final String charsetName) throws IOException {
        try (InputStream is = getAsInputStream(path)) {
            return IOUtils.toString(is, charsetName);
        }
    }

    /**
     * Get a Manta object's data as a {@link String} using the specified encoding.
     * This method is not memory efficient, by loading the data into a String you are
     * loading all of the Object's data into memory.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param charset The encoding type used to convert bytes from the
     *                stream into characters to be scanned
     * @return String containing the entire Manta object
     * @throws IOException when there is a problem getting the object over the network
     */
    public String getAsString(final String path, final Charset charset) throws IOException {
        try (InputStream is = getAsInputStream(path)) {
            return IOUtils.toString(is, charset);
        }
    }

    /**
     * Copies Manta object's data to a temporary file on the file system and return
     * a reference to the file using a NIO {@link Path}. This method is memory
     * efficient because it uses streams to do the copy.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return reference to the temporary file as a {@link Path} object
     * @throws IOException when there is a problem getting the object over the network
     */
    public Path getToTempPath(final String path) throws IOException {
        try (InputStream is = getAsInputStream(path)) {
            final Path temp = Files.createTempFile("manta-object", "tmp");

            Files.copy(is, temp, StandardCopyOption.REPLACE_EXISTING);

            return temp;
        }
    }

    /**
     * Copies Manta object's data to a temporary file on the file system and return
     * a reference to the file using a {@link File}. This method is memory
     * efficient because it uses streams to do the copy.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return reference to the temporary file as a {@link File} object
     * @throws IOException when there is a problem getting the object over the network
     */
    public File getToTempFile(final String path) throws IOException {
        return getToTempPath(path).toFile();
    }

    /**
     * Get a Manta object's data as an NIO {@link java.nio.channels.SeekableByteChannel}.
     * This method allows you to stream data from the Manta storage service in a
     * memory efficient manner to your application. Unlike an {@link InputStream},
     * this will allow you to stream data by moving between arbitrary position in
     * the Manta object data.
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param position The starting position (in number of bytes) to read from
     * @return seekable stream of object data
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaSeekableByteChannel getSeekableByteChannel(
            final String rawPath, final long position) throws IOException {
        Validate.notNull(rawPath, "Path must not be null");
        String path = formatPath(rawPath);

        return new MantaSeekableByteChannel(path, position,
                this.connectionFactory, this.httpHelper);
    }

    /**
     * Get a Manta object's data as an NIO {@link java.nio.channels.SeekableByteChannel}.
     * This method allows you to stream data from the Manta storage service in
     * a memory efficient manner to your application. Unlike an {@link InputStream},
     * this will allow you to stream data by moving between arbitrary position
     * in the Manta object data.
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return seekable stream of object data
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaSeekableByteChannel getSeekableByteChannel(final String rawPath)
            throws IOException {
        Validate.notNull(rawPath, "Path must not be null");
        String path = formatPath(rawPath);

        return new MantaSeekableByteChannel(path, this.connectionFactory,
                this.httpHelper);
    }

    /**
     * <p>Generates a URL that allows for the download of the resource specified
     * in the path without any additional authentication.</p>
     *
     * <p>This could be useful for when you want to generate links that expire
     * after N seconds to give to external users for temporary download
     * access.</p>
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param method the String GET or the string HEAD. This is the HTTP verb
     *               that will be used when requesting this resource
     * @param expiresIn time from now on when resource expires
     * @return a signed URL that allows for downloading a resource
     * @throws IOException thrown if there is a problem generating the URL
     */
    public URI getAsSignedURI(final String path, final String method,
                              final TemporalAmount expiresIn)
            throws IOException {
        Validate.notNull(expiresIn, "expires in duration must not be null");
        final Instant expires = Instant.now().plus(expiresIn);
        return getAsSignedURI(path, method, expires);
    }

    /**
     * <p>Generates a URL that allows for the download of the resource specified
     * in the path without any additional authentication.</p>
     *
     * <p>This could be useful for when you want to generate links that expire
     * after N seconds to give to external users for temporary download
     * access.</p>
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param method the String GET or the string HEAD. This is the HTTP verb
     *               that will be used when requesting this resource
     * @param expires time when resource expires and become unavailable
     * @return a signed URL that allows for downloading a resource
     * @throws IOException thrown if there is a problem generating the URL
     */
    public URI getAsSignedURI(final String path, final String method,
                              final Instant expires)
            throws IOException {
        Validate.notNull(expires, "Expires setting must not be null");

        return getAsSignedURI(path, method, expires.getEpochSecond());
    }

    /**
     * <p>Generates a URL that allows for the download of the resource specified
     * in the path without any additional authentication.</p>
     *
     * <p>This could be useful for when you want to generate links that expire
     * after N seconds to give to external users for temporary download
     * access.</p>
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param method the String GET or the string HEAD. This is the HTTP verb
     *               that will be used when requesting this resource
     * @param expiresEpochSeconds time when resource expires and become unavailable in epoch seconds
     * @return a signed URL that allows for downloading a resource
     * @throws IOException thrown if there is a problem generating the URL
     */
    public URI getAsSignedURI(final String path, final String method,
                              final long expiresEpochSeconds)
            throws IOException {
        Validate.notNull(path, "Path must be not be null");

        final String fullPath = String.format("%s%s", this.url, formatPath(path));
        final URI request = URI.create(fullPath);

        return uriSigner.signURI(request, method, expiresEpochSeconds);
    }

    /**
     * Get the metadata associated with a Manta object.
     *
     * @param rawPath The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectResponse}.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse head(final String rawPath) throws IOException {
        Validate.notBlank(rawPath, "Path must not be empty nor null");

        String path = formatPath(rawPath);
        final HttpResponse response = httpHelper.httpHead(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders(response.getAllHeaders());
        return new MantaObjectResponse(path, headers);
    }

    /**
     * Return a stream of the contents of a directory in Manta as an {@link Iterator}.
     *
     * @param path The fully qualified path of the directory.
     * @return A {@link Iterator} of {@link MantaObjectResponse} listing the contents of the directory.
     */
    public MantaDirectoryListingIterator streamingIterator(final String path) {
        return streamingIterator(path, MAX_RESULTS);
    }

    /**
     * Return a stream of the contents of a directory in Manta as an {@link Iterator}.
     *
     * @param path The fully qualified path of the directory.
     * @param pagingSize size of result set requested against the Manta API (2-1024)
     * @return A {@link Iterator} of {@link MantaObjectResponse} listing the contents of the directory.
     */
    public MantaDirectoryListingIterator streamingIterator(final String path, final int pagingSize) {
        MantaDirectoryListingIterator itr =
            new MantaDirectoryListingIterator(this.url, path, httpHelper, pagingSize);
        danglingStreams.add(itr);
        return itr;
    }

    /**
     * Return a stream of the contents of a directory in Manta.
     *
     * @param path The fully qualified path of the directory.
     * @return A {@link Stream} of {@link MantaObjectResponse} listing the contents of the directory.
     * @throws IOException thrown when there is a problem getting the listing over the network
     */
    public Stream<MantaObject> listObjects(final String path) throws IOException {
        final MantaDirectoryListingIterator itr = streamingIterator(path);

        /* We preemptively check the iterator for a next value because that will
         * trigger an error if the path doesn't exist or is otherwise inaccessible.
         * This error typically takes the form of an UncheckedIOException, so we
         * unwind that exception if the cause is a MantaClientHttpResponseException
         * and rethrow another MantaClientHttpResponseException, so that the
         * stacktrace will point to this running method.
         */
        try {
            if (!itr.hasNext()) {
                return Stream.empty();
            }
        } catch (UncheckedIOException e) {
            if (e.getCause() instanceof MantaClientHttpResponseException) {
                throw e.getCause();
            } else {
                throw e;
            }
        }

        Stream<Map<String, Object>> backingStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        itr, Spliterator.ORDERED | Spliterator.NONNULL), false);

        Stream<MantaObject> stream = backingStream.map(item -> {
            String name = Objects.toString(item.get("name"));
            String mtime = Objects.toString(item.get("mtime"));
            String type = Objects.toString(item.get("type"));
            Validate.notNull(name, "File name must not be null");
            String objPath = String.format("%s%s%s",
                    StringUtils.removeEnd(path, SEPARATOR),
                    SEPARATOR,
                    StringUtils.removeStart(name, SEPARATOR));
            MantaHttpHeaders headers = new MantaHttpHeaders();
            headers.setLastModified(mtime);

            if (type.equals("directory")) {
                headers.setContentType(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);
            } else {
                headers.setContentType(ContentType.APPLICATION_OCTET_STREAM.toString());
            }

            if (item.containsKey("etag")) {
                headers.setETag(Objects.toString(item.get("etag")));
            }

            if (item.containsKey("size")) {
                long size = Long.parseLong(Objects.toString(item.get("size")));
                headers.setContentLength(size);
            }

            if (item.containsKey("durability")) {
                String durabilityString = Objects.toString(item.get("durability"));
                if (durabilityString != null) {
                    int durability = Integer.parseInt(durabilityString);
                    headers.setDurabilityLevel(durability);
                }
            }

            return new MantaObjectResponse(formatPath(objPath), headers);
        });

        danglingStreams.add(stream);

        return stream;
    }

    /**
     * Return a boolean indicating if a directory is empty.
     *
     * @param path directory path
     * @return true if directory is empty, otherwise false
     * @throws IOException thrown when we are unable to list the directory over the network
     */
    public boolean isDirectoryEmpty(final String path) throws IOException {
        final MantaObject object = this.head(path);

        if (!object.isDirectory()) {
            MantaClientException e = new MantaClientException("The requested object was not a directory");
            e.setContextValue("path", path);
            throw e;
        }

        Long size = object.getHttpHeaders().getResultSetSize();

        if (size == null) {
            MantaClientException e = new MantaClientException(
                "Expected result-set-size header to be non-null but it was not"
                        + " part of the response");
            e.setContextValue("path", path);
            throw e;
        }

        return size == 0;
    }

    /**
     * Convenience method that issues a HTTP HEAD request to see if a given
     * object exists at the specified path.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return true if Manta returns a 2xx status code and false if Manta returns
     *         any error status code or if there was a connection problem (IOException)
     */
    public boolean existsAndIsAccessible(final String path) {
        try {
            head(path);
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    /**
     * Puts an object into Manta.
     *
     * @param path    The path to the Manta object.
     * @param source  {@link InputStream} to copy object data from
     * @param headers optional HTTP headers to include when copying the object
     * @return Manta response object
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaHttpHeaders headers) throws IOException {
        return put(path, source, headers, null);
    }

    /**
     * Puts an object into Manta.
     *
     * @param path     The path to the Manta object.
     * @param source   {@link InputStream} to copy object data from
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaMetadata metadata) throws IOException {
        return put(path, source, null, metadata);
    }

    /**
     * Puts an object into Manta using a stream with an unknown length. Since
     * we don't know the length,
     *
     * @param path     The path to the Manta object.
     * @param source   {@link InputStream} to copy object data from
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        return put(path, source, -1L, headers, metadata);
    }

    /**
     * Puts an object into Manta.
     *
     * @param rawPath The path to the Manta object.
     * @param source {@link InputStream} to copy object data from
     * @param contentLength the total length of the stream (-1 if unknown)
     * @param headers optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String rawPath,
                                   final InputStream source,
                                   final long contentLength,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Validate.notNull(rawPath, "rawPath must not be null");
        Validate.notNull(source, "Input stream must not be null");
        final String path = formatPath(rawPath);

        final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(headers,
                ContentType.APPLICATION_OCTET_STREAM);

        final int preLoadSize = config.getUploadBufferSize();
        final HttpEntity entity;

        /* We don't know how big the stream is, so we read N bytes from it and
         * see if it ends. If it ended, then we just convert that buffer into
         * an entity and pass it. If it didn't end, then we create new stream
         * that concatenates the bytes read with the source stream.
         * Unfortunately, this will put us in a chunked transfer encoding and
         * it will affect performance. */
        if (contentLength < 0) {
            // If our stream is a FileInputStream, then we can pull the size off of it
            if (source.getClass().equals(FileInputStream.class)) {
                FileInputStream fsin = (FileInputStream)source;
                entity = new InputStreamEntity(fsin, fsin.getChannel().size(), contentType);
            } else {
                byte[] preLoad = new byte[preLoadSize];
                int read = IOUtils.read(source, preLoad);

                // The total amount of bytes read was less than the preload size,
                // so we can just return a in-memory non-streaming entity
                if (read < preLoadSize) {
                    entity = new ExposedByteArrayEntity(preLoad, 0, read, contentType);
                } else {
                    ByteArrayInputStream bin = new ByteArrayInputStream(preLoad);
                    SequenceInputStream sin = new SequenceInputStream(bin, source);

                    entity = new InputStreamEntity(sin, contentType);
                }

            }
        /* We know how big the stream is, so we can decide if it is within our
         * preload threshold and load it into memory or if it isn't within the
         * threshold, we can pass it on as a streamed entity in non-chunked mode. */
        } else {
            if (contentLength <= preLoadSize && contentLength <= Integer.MAX_VALUE) {
                byte[] preLoad = new byte[(int)contentLength];
                IOUtils.read(source, preLoad);
                entity = new ExposedByteArrayEntity(preLoad, contentType);
            } else {
                entity = new InputStreamEntity(source, contentLength, contentType);
            }
        }

        return httpHelper.httpPut(path, headers, entity, metadata);
    }

    /**
     * Copies the supplied {@link InputStream} to a remote Manta object at the
     * specified path.
     *
     * @param path   The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param source the {@link InputStream} to copy data from
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path, final InputStream source) throws IOException {
        return put(path, source, null, null);
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path    The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string  string to copy
     * @param headers optional HTTP headers to include when copying the object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string,
                                   final MantaHttpHeaders headers) throws IOException {
        return put(path, string, headers, null);
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string   string to copy
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string,
                                   final MantaMetadata metadata) throws IOException {
        return put(path, string, null, metadata);
    }

    /**
     * Creates an OutputStream that wraps a PUT request to Manta. Try to avoid using this
     * to add data to Manta because it requires an additional thread to be started in order
     * to upload using an {@link java.io.OutputStream}. Additionally, if you do not close()
     * the stream, the data will not be uploaded.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return A OutputStream that allows for directly uploading to Manta
     */
    public MantaObjectOutputStream putAsOutputStream(final String path) {
        return putAsOutputStream(path, null, null);
    }

    /**
     * Creates an OutputStream that wraps a PUT request to Manta. Try to avoid using this
     * to add data to Manta because it requires an additional thread to be started in order
     * to upload using an {@link java.io.OutputStream}. Additionally, if you do not close()
     * the stream, the data will not be uploaded.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers  optional HTTP headers to include when copying the object
     * @return A OutputStream that allows for directly uploading to Manta
     */
    public MantaObjectOutputStream putAsOutputStream(final String path,
                                                     final MantaHttpHeaders headers) {
        return putAsOutputStream(path, headers, null);
    }

    /**
     * Creates an OutputStream that wraps a PUT request to Manta. Try to avoid using this
     * to add data to Manta because it requires an additional thread to be started in order
     * to upload using an {@link java.io.OutputStream}. Additionally, if you do not close()
     * the stream, the data will not be uploaded.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param metadata optional user-supplied metadata for object
     * @return A OutputStream that allows for directly uploading to Manta
     */
    public MantaObjectOutputStream putAsOutputStream(final String path,
                                                     final MantaMetadata metadata) {
        return putAsOutputStream(path, null, metadata);
    }

    /**
     * Creates an OutputStream that wraps a PUT request to Manta. Try to avoid using this
     * to add data to Manta because it requires an additional thread to be started in order
     * to upload using an {@link java.io.OutputStream}. Additionally, if you do not close()
     * the stream, the data will not be uploaded.
     *
     * @param rawPath     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param metadata optional user-supplied metadata for object
     * @param headers  optional HTTP headers to include when copying the object
     * @return A OutputStream that allows for directly uploading to Manta
     */
    public MantaObjectOutputStream putAsOutputStream(final String rawPath,
                                                     final MantaHttpHeaders headers,
                                                     final MantaMetadata metadata) {
        Validate.notNull(rawPath, "rawPath must not be null");
        final String path = formatPath(rawPath);

        final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(headers,
                path, ContentType.APPLICATION_OCTET_STREAM);

        MantaObjectOutputStream stream = new MantaObjectOutputStream(path,
                this.httpHelper, headers, metadata, contentType);

        danglingStreams.add(stream);

        return stream;
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param rawPath     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string   string to copy
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String rawPath,
                                   final String string,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Validate.notNull(string, "String content must not be null");
        Validate.notNull(rawPath, "Path must not be null");

        String path = formatPath(rawPath);

        final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(
                headers,
                ContentType.APPLICATION_OCTET_STREAM);

        /* We remove the content-type from the headers, because
         * it will be automatically from the entity. Adding it twice
         * can confuse our contract. */
        if (headers != null) {
            headers.remove(HttpHeaders.CONTENT_TYPE);
        }

        final HttpEntity entity;

        if (string == null) {
            entity = null;
        } else {
            entity = new ExposedStringEntity(string, contentType);
        }

        return httpHelper.httpPut(path, headers, entity, metadata);
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path   The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string string to copy
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string) throws IOException {
        return put(path, string, Charset.defaultCharset());
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path   The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string string to copy
     * @param charsetName character set to encode string as
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string,
                                   final String charsetName) throws IOException {
        return put(path, string, Charset.forName(charsetName));
    }

    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path   The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string string to copy
     * @param charset character set to encode string as
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string,
                                   final Charset charset) throws IOException {
        ContentType contentType = ContentType.TEXT_PLAIN.withCharset(charset);
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setContentType(contentType.toString());

        return put(path, string, headers, null);
    }

    /**
     * Copies the supplied {@link File} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param file     file to upload
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final File file) throws IOException {
        return put(path, file, null, null);
    }

    /**
     * Copies the supplied {@link File} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param file     file to upload
     * @param headers  optional HTTP headers to include when copying the object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final File file,
                                   final MantaHttpHeaders headers) throws IOException {
        return put(path, file, headers, null);
    }

    /**
     * Copies the supplied {@link File} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param file     file to upload
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final File file,
                                   final MantaMetadata metadata) throws IOException {
        return put(path, file, null, metadata);
    }

    /**
     * Copies the supplied {@link File} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param rawPath     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param file     file to upload
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String rawPath,
                                   final File file,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Validate.notNull(rawPath, "rawPath must not be null");
        Validate.notNull(file, "File must not be null");
        final String path = formatPath(rawPath);

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

        final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(headers,
                path, file, ContentType.APPLICATION_OCTET_STREAM);

        final HttpEntity entity = new FileEntity(file, contentType);

        return httpHelper.httpPut(path, headers, entity, metadata);
    }

    /**
     * Copies the supplied byte array to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param bytes    byte array to upload
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final byte[] bytes) throws IOException {
        return put(path, bytes, null, null);
    }

    /**
     * Copies the supplied byte array to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param bytes    byte array to upload
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final byte[] bytes,
                                   final MantaMetadata metadata) throws IOException {
        return put(path, bytes, null, metadata);
    }

    /**
     * Copies the supplied byte array to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param bytes    byte array to upload
     * @param headers  optional HTTP headers to include when copying the object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final byte[] bytes,
                                   final MantaHttpHeaders headers) throws IOException {
        return put(path, bytes, headers, null);
    }

    /**
     * Copies the supplied byte array to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param rawPath     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param bytes    byte array to upload
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String rawPath,
                                   final byte[] bytes,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Validate.notNull(rawPath, "rawPath must not be null");
        Validate.notNull(bytes, "Byte array must not be null");
        final String path = formatPath(rawPath);

        final ContentType contentType = ContentTypeLookup.findOrDefaultContentType(
                headers, path, ContentType.APPLICATION_OCTET_STREAM);

        final HttpEntity entity = new ExposedByteArrayEntity(bytes, contentType);

        return httpHelper.httpPut(path, headers, entity, metadata);
    }

    /**
     * Appends the specified metadata to an existing Manta object.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param metadata user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the metadata over the network
     */
    public MantaObjectResponse putMetadata(final String path, final MantaMetadata metadata)
            throws IOException {
        final MantaHttpHeaders headers = new MantaHttpHeaders(metadata);
        return putMetadata(path, headers, metadata);
    }

    /**
     * Appends metadata derived from HTTP headers to an existing Manta object.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers  HTTP headers to include when copying the object
     * @return Manta response object
     * @throws IOException when there is a problem sending the metadata over the network
     */
    public MantaObjectResponse putMetadata(final String path, final MantaHttpHeaders headers)
            throws IOException {
        Validate.notNull(headers, "Headers must not be null");

        final MantaMetadata metadata = new MantaMetadata(headers.metadataAsStrings());
        return putMetadata(path, headers, metadata);
    }

    /**
     * Replaces the specified metadata to an existing Manta object using the
     * specified HTTP headers.
     *
     * @param rawPath     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers  HTTP headers to include when copying the object
     * @param metadata user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the metadata over the network
     */
    public MantaObjectResponse putMetadata(final String rawPath,
                                           final MantaHttpHeaders headers,
                                           final MantaMetadata metadata)
            throws IOException {
        Validate.notNull(headers, "Headers must not be null");
        Validate.notNull(metadata, "Metadata must not be null");

        for (String header : ILLEGAL_METADATA_HEADERS) {
            if (headers.containsKey(header)) {
                String msg = String.format("Critical header [%s] can't be changed", header);
                throw new IllegalArgumentException(msg);
            }
        }

        String path = formatPath(rawPath);
        return httpHelper.httpPutMetadata(path, headers, metadata);
    }

    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @return true when directory was created
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public boolean putDirectory(final String path) throws IOException {
        return putDirectory(path, null);
    }

    /**
     * Creates a directory in Manta.
     *
     * @param rawPath    The fully qualified path of the Manta directory.
     * @param rawHeaders Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @return true when directory was created
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public boolean putDirectory(final String rawPath, final MantaHttpHeaders rawHeaders)
            throws IOException {
        Validate.notBlank(rawPath, "PUT directory path must not be empty nor null");

        String path = formatPath(rawPath);

        LOG.debug("PUT    {} [directory]", path);

        final HttpPut put = connectionFactory.put(path);

        final MantaHttpHeaders headers;

        if (rawHeaders == null) {
            headers = new MantaHttpHeaders();
        } else {
            headers = rawHeaders;
        }

        put.setHeaders(headers.asApacheHttpHeaders());
        put.setHeader(HttpHeaders.CONTENT_TYPE, MantaContentTypes.DIRECTORY_LIST.getContentType());

        HttpResponse response = httpHelper.executeAndCloseRequest(put,
                HttpStatus.SC_NO_CONTENT,
                "PUT    {} response [{}] {} ");

        // When LastModified is set, the directory already exists
        return response.getFirstHeader(HttpHeaders.LAST_MODIFIED) == null;
    }

    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @param recursive recursive create all of the directories specified in the path
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String path, final boolean recursive)
            throws IOException {
        putDirectory(path, recursive, null);
    }

    /**
     * Creates a directory in Manta.
     *
     * @param rawPath The fully qualified path of the Manta directory.
     * @param recursive recursive create all of the directories specified in the path
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String rawPath, final boolean recursive,
                             final MantaHttpHeaders headers)
            throws IOException {

        if (!recursive) {
            putDirectory(rawPath, headers);
            return;
        }

        final String[] parts = rawPath.split(SEPARATOR);
        final Iterator<Path> itr = Paths.get("", parts).iterator();
        final StringBuilder sb = new StringBuilder(SEPARATOR);

        for (int i = 0; itr.hasNext(); i++) {
            final String part = itr.next().toString();
            sb.append(part);

            // This means we aren't in the home nor in the reserved
            // directory path (stor, public, jobs, etc)
            if (i > 1) {
                putDirectory(sb.toString(), headers);
            }

            if (itr.hasNext()) {
                sb.append(SEPARATOR);
            }
        }
    }

    /**
     * Create a Manta snaplink.
     *
     * @param rawLinkPath The fully qualified path of the new snaplink.
     * @param rawObjectPath The fully qualified path of the object to link against.
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putSnapLink(final String rawLinkPath, final String rawObjectPath,
                            final MantaHttpHeaders headers)
            throws IOException {
        final String linkPath = formatPath(rawLinkPath);
        final String objectPath = formatPath(rawObjectPath);

        LOG.debug("PUT    {} -> {} [snaplink]", objectPath, linkPath);
        final HttpPut put = connectionFactory.put(linkPath);

        if (headers != null) {
            put.setHeaders(headers.asApacheHttpHeaders());
        }

        put.setHeader(HttpHeaders.CONTENT_TYPE, MantaContentTypes.SNAPLINK.getContentType());
        put.setHeader(HttpHeaders.LOCATION, objectPath);

        httpHelper.executeAndCloseRequest(put, HttpStatus.SC_NO_CONTENT,
                "PUT    {} -> {} response [{}] {} ",
                objectPath, linkPath);
    }

    /**
     * <p>Moves an object from one path to another path. When moving
     * directories or files between different directories, this operation is
     * not transactional and may fail or produce inconsistent result if
     * the source or the destination is modified while the operation is
     * in progress.</p>
     *
     * <p><em>Note: If you need to do a rename, then use
     * {@link #putSnapLink(String, String, MantaHttpHeaders)}</em></p>
     *
     * @param source Original path to move from
     * @param destination Destination path to move to
     * @throws IOException thrown when something goes wrong
     */
    public void move(final String source, final String destination)
            throws IOException {
        move(source, destination, false);
    }

    /**
     * <p>Moves an object from one path to another path. When moving
     * directories or files between different directories, this operation is
     * not transactional and may fail or produce inconsistent result if
     * the source or the destination is modified while the operation is
     * in progress.</p>
     *
     * <p><em>Note: If you need to do a rename, then use
     * {@link #putSnapLink(String, String, MantaHttpHeaders)}</em></p>
     *
     * @param source Original path to move from
     * @param destination Destination path to move to
     * @param recursivelyCreateDestinationDirectories when true create the full destination directory path
     * @throws IOException thrown when something goes wrong
     */
    public void move(final String source, final String destination,
                     final boolean recursivelyCreateDestinationDirectories)
            throws IOException {
        Validate.notBlank(source, "Source path must not be empty nor null");
        Validate.notBlank(destination, "Destination path must not be empty nor null");

        LOG.debug("Moving [{}] to [{}]", source, destination);

        MantaObjectResponse entry = head(source);

        if (entry.isDirectory()) {
            moveDirectory(source, destination, entry);
        } else {
            moveFile(source, destination, recursivelyCreateDestinationDirectories);
        }
    }

    /**
     * Moves a file from one path to another path. When moving
     * files between different directories, this operation is
     * not transactional and may fail or produce inconsistent result if
     * the source or the destination is modified while the operation is
     * in progress.
     *
     * @param source Original path to move from
     * @param destination Destination path to move to
     * @param recursivelyCreateDestinationDirectories when true create the full destination directory path
     *
     * @throws IOException thrown when something goes wrong
     */
    private void moveFile(final String source, final String destination,
                          final boolean recursivelyCreateDestinationDirectories)
            throws IOException {
        final String formattedDestination = formatPath(destination);
        final String destinationDir = FilenameUtils.getFullPath(formattedDestination);

        if (recursivelyCreateDestinationDirectories && !existsAndIsAccessible(destinationDir)) {
                putDirectory(destinationDir, true);
        }

        putSnapLink(destination, source, null);
        delete(source);
    }

    /**
     * Moves a directory from one path to another path. This operation is not
     * transactional and may fail or produce inconsistent result if the source
     * or the destination is modified while the operation is in progress.
     *
     * @param source Original path to move from
     * @param destination Destination path to move to
     * @param entry Directory supplemental data object
     * @throws IOException thrown when something goes wrong
     */
    private void moveDirectory(final String source, final String destination,
                               final MantaObjectResponse entry)
            throws IOException {
        putDirectory(destination, true);
        MantaHttpHeaders sourceHeaders = entry.getHttpHeaders();
        Long contentsCount = sourceHeaders.getResultSetSize();

        // If we were just copying an empty directory, we just create the
        // new directory and delete the original
        if (contentsCount != null && contentsCount == 0L) {
            delete(source);
            return;
        }

        MantaObjectResponse destDir = head(destination);
        String destDirPath = destDir.getPath();
        String sourceDirPath = entry.getPath();

        listObjects(source).forEach(mantaObject -> {
            try {
                String sourcePath = mantaObject.getPath();
                String relPath = sourcePath.substring(sourceDirPath.length());
                String destFullPath = destDirPath + SEPARATOR + relPath;

                if (mantaObject.isDirectory()) {
                    move(mantaObject.getPath(), destFullPath);
                } else {
                    putSnapLink(destFullPath, sourcePath, null);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        deleteRecursive(source);
    }

    /**
      * Method that returns the configuration context used to
      * instantiate the MantaClient instance.
      *
      * @return instance of the configuration context used to construct this client
      */
    public ConfigContext getContext() {
        return this.config;
    }

    /* ======================================================================
     * Job Methods
     * ====================================================================== */

    /**
     * Submits a new job to be executed. This call is not idempotent, so calling
     * it twice will create two jobs.
     *
     * @param job Object populated with details about the request job
     * @return id of the newly created job
     * @throws IOException thrown when there are problems creating the job over the network
     */
    public UUID createJob(final MantaJob job) throws IOException {
        Validate.notNull(job, "Manta job must not be null");

        String path = formatPath(String.format("%s/jobs", home));
        ObjectMapper mapper = MantaObjectMapper.INSTANCE;
        byte[] json = mapper.writeValueAsBytes(job);

        HttpEntity entity = new ExposedByteArrayEntity(json,
                ContentType.APPLICATION_JSON);

        HttpPost post = connectionFactory.post(path);
        post.setEntity(entity);

        Function<CloseableHttpResponse, UUID> jobIdFunction = response -> {
            final String location = response.getFirstHeader(HttpHeaders.LOCATION).getValue();
            String id = MantaUtils.lastItemInPath(location);
            return UUID.fromString(id);
        };

        /* This endpoint has a propensity for failing to respond, so we retry on
         * failure. */

        final int retries;

        if (config.getRetries() == null) {
            retries = DefaultsConfigContext.DEFAULT_HTTP_RETRIES;
        } else {
            retries = config.getRetries();
        }

        IOException lastException = new IOException("Never thrown. Report me as a bug.");

        // if retries are set to zero, we always execute at least once
        for (int count = 0; count < retries || count == 0; count++) {
            try {
                return httpHelper.executeAndCloseRequest(post,
                        jobIdFunction, "POST   {} response [{}] {} ", path);
            } catch (NoHttpResponseException e) {
                lastException = e;
                LOG.warn("Error posting createJob. Retrying.", e);
            }
        }

        throw lastException;
    }

    /**
     * Submits inputs to an already created job, as created by createJob().
     * Inputs are object names, and are fed in as a \n separated stream.
     * Inputs will be processed as they are received.
     *
     * @param jobId UUID of the Manta job
     * @param inputs iterator of paths to Manta objects to be added as inputs
     * @throws IOException thrown when we are unable to add inputs over the network
     */
    public void addJobInputs(final UUID jobId,
                             final Iterator<String> inputs) throws IOException {
        Validate.notNull(inputs, "Inputs must not be null");

        ContentType contentType = ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8);
        HttpEntity entity = new StringIteratorHttpContent(inputs, contentType);

        addJobInputs(jobId, entity);
    }

    /**
     * Submits inputs to an already created job, as created by createJob().
     * Inputs are object names, and are fed in as a \n separated stream.
     * Inputs will be processed as they are received.
     *
     * @param jobId UUID of the Manta job
     * @param inputs stream of paths to Manta objects to be added as inputs
     * @throws IOException thrown when we are unable to add inputs over the network
     */
    public void addJobInputs(final UUID jobId,
                             final Stream<String> inputs) throws IOException {
        Validate.notNull(inputs, "Inputs must not be null");

        ContentType contentType = ContentType.TEXT_PLAIN.withCharset(StandardCharsets.UTF_8);
        HttpEntity entity = new StringIteratorHttpContent(inputs, contentType);

        addJobInputs(jobId, entity);
    }

    /**
     * Submits inputs to an already created job, as created by createJob().
     * Inputs are object names, and are fed in as a \n separated stream.
     * Inputs will be processed as they are received.
     *
     * @param jobId UUID of the Manta job
     * @param entity Http entity to use for collecting streaming content
     * @throws IOException thrown when we are unable to add inputs over the network
     */
    private void addJobInputs(final UUID jobId,
                              final HttpEntity entity) throws IOException {
        Validate.notNull(jobId, "Manta job id must not be null");
        processJobInputs(jobId, entity);
    }

    /**
     * Utility method for processing the addition of job inputs over HTTP.
     *
     * @param jobId UUID of the Manta job
     * @param entity content object containing input objects
     * @throws IOException thrown when we are unable to add inputs over the network
     */
    protected void processJobInputs(final UUID jobId,
                                    final HttpEntity entity)
            throws IOException {

        String path = String.format("%s/jobs/%s/live/in", home, jobId);

        HttpPost post = connectionFactory.post(path);
        post.setHeader(HttpHeaders.CONTENT_ENCODING, "chunked");
        post.setEntity(entity);

        httpHelper.executeAndCloseRequest(post, "POST   {} response [{}] {} ");
    }

    /**
     * <p>Get a stream of all of the input objects submitted for a job.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     *
     * @param jobId UUID of the Manta job
     * @return Stream of input objects associated with a job
     * @throws IOException thrown when there is a problem getting objects over the network
     */
    public Stream<String> getJobInputs(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/in", home, jobId);

        HttpGet get = connectionFactory.get(path);
        HttpResponse response = httpHelper.executeRequest(get,
                "GET    {} response [{}] {} ");
        return responseAsStream(response);
    }

    /**
     * Submits inputs to an already created job, as created by CreateJob.
     *
     * @param jobId UUID of the Manta job
     * @return true when Manta has accepted the ending input
     * @throws IOException thrown when there is a problem ending input over the network
     */
    public boolean endJobInput(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/in/end", home, jobId);

        HttpResponse response = httpHelper.httpPost(path);
        StatusLine statusLine = response.getStatusLine();

        // We expect a return value of 202 when the cancel request was accepted
        return statusLine.getStatusCode() == HttpStatus.SC_ACCEPTED;
    }

    /**
     * <p>This cancels a job from doing any further work. Cancellation is
     * asynchronous and "best effort"; there is no guarantee the job will
     * actually stop. For example, short jobs where input is already
     * closed will likely still run to completion. This is however useful
     * when:</p>
     * <ul>
     * <li>input is still open</li>
     * <li>you have a long-running job</li>
     * </ul>
     * @param jobId UUID of the Manta job
     * @return true when Manta has accepted the cancel request
     * @throws IOException thrown when we have a problem canceling over the network
     */
    public boolean cancelJob(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/cancel",
                home, jobId);

        HttpResponse response = httpHelper.httpPost(path);
        StatusLine statusLine = response.getStatusLine();

        // We expect a return value of 202 when the cancel request was accepted
        return statusLine.getStatusCode() == HttpStatus.SC_ACCEPTED;
    }

    /**
     * Gets the high-level job container object for a given id.
     * First, attempts to get the live status object and if it can't be
     * retrieved, then we get the archived status object.
     *
     * @param jobId UUID of the Manta job
     * @return Object representing the properties of the job or null if not found
     * @throws IOException thrown when we can't get the job over the network
     */
    public MantaJob getJob(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Manta job id must not be null");
        final String livePath = String.format("%s/jobs/%s/live/status",
                home, jobId);

        final CloseableHttpClient client = connectionContext.getHttpClient();
        final HttpUriRequest initialRequest = connectionFactory.get(livePath);
        MantaJob job;
        HttpEntity entity;

        CloseableHttpResponse lastResponse = null;

        try (CloseableHttpResponse initialResponse = client.execute(initialRequest)) {
            lastResponse = initialResponse;
            final StatusLine statusLine = initialResponse.getStatusLine();

            // If we can't get the live status of the job, we try to get the archived
            // status of the job just like the CLI mjob utility.
            if (statusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                final String archivePath = String.format("%s/jobs/%s/job.json",
                        home, jobId);

                final HttpUriRequest archiveRequest = connectionFactory.get(archivePath);

                // We close the request that was previously opened, because it
                // didn't have what we need.
                IOUtils.closeQuietly(initialResponse);

                CloseableHttpResponse archiveResponse = client.execute(archiveRequest);
                lastResponse = archiveResponse;
                final StatusLine archiveStatusLine = archiveResponse.getStatusLine();

                // Job wasn't available via live status nor archive
                if (archiveStatusLine.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                    String msg = "No record for job in Manta";
                    MantaJobException e = new MantaJobException(jobId, msg);
                    HttpHelper.annotateContextedException(e, archiveRequest, archiveResponse);
                    throw e;
                    // There was an undefined problem with pulling the job from the archive
                } else if (archiveStatusLine.getStatusCode() != HttpStatus.SC_OK) {
                    String msg = "Unable to get job data from archive";
                    MantaIOException ioe = new MantaIOException(msg);
                    HttpHelper.annotateContextedException(ioe, archiveRequest, archiveResponse);
                    ioe.setContextValue("jobId", Objects.toString(jobId));
                    throw ioe;
                    // The job was pulled without problems from the archive
                } else {
                    entity = archiveResponse.getEntity();
                }

            // There was an undefined problem with pulling the job from the live status
            } else if (statusLine.getStatusCode() != HttpStatus.SC_OK) {
                String msg = "Unable to get job data from live status";
                MantaIOException ioe = new MantaIOException(msg);
                HttpHelper.annotateContextedException(ioe, initialRequest, initialResponse);
                ioe.setContextValue("jobId", Objects.toString(jobId));
                throw ioe;
            // The job was pulled without problems from the live status
            } else {
                entity = initialResponse.getEntity();
            }

            try (InputStream in = entity.getContent()) {
                job = MantaObjectMapper.INSTANCE.readValue(in, MantaJob.class);
            } catch (IOException e) {
                String msg = "Unable to deserialize job data";
                MantaIOException ioe = new MantaIOException(msg, e);
                HttpHelper.annotateContextedException(ioe, initialRequest, initialResponse);
                ioe.setContextValue("jobId", Objects.toString(jobId));
                throw ioe;
            }
        // We sweep up any uncaught io exceptions and wrap them with details
        } catch (IOException e) {
            if (e instanceof MantaIOException) {
                throw e;
            }

            String msg = "Unable to get job data";
            MantaIOException ioe = new MantaIOException(msg, e);
            HttpHelper.annotateContextedException(ioe, initialRequest, lastResponse);
            ioe.setContextValue("jobId", Objects.toString(jobId));
            throw ioe;
        // Likewise we sweep up any uncaught runtime exceptions and wrap them
        } catch (RuntimeException e) {
            if (e instanceof MantaException) {
                throw e;
            }

            String msg = "Unexpected error when getting job data";
            MantaJobException je = new MantaJobException(jobId, msg, e);
            HttpHelper.annotateContextedException(je, initialRequest, lastResponse);
            throw je;
        } finally {
            if (lastResponse != null) {
                IOUtils.closeQuietly(lastResponse);
            }
        }

        Validate.notNull("Job returned must not be null");
        return job;
    }

    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @return a stream with all of the jobs
     */
    public Stream<MantaJob> getAllJobs() {
        return getAllJobIds().map(id -> {
            Validate.notNull(id, "Job ids must not be null");

            try {
                return getJob(id);
            } catch (IOException e) {
                String msg = "Error processing job object stream";
                MantaJobException jobException = new MantaJobException(id, msg, e);
                jobException.setContextValue("failedJobId", Objects.toString(id));
                throw jobException;
            }
        });
    }

    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} that matches
     * the supplied name from the Manta API. <strong>Make sure to close this stream
     * when you are done with otherwise the HTTP socket will remain open.</strong>
     *
     * @param limit the maximum number of jobs to list 0-1024
     * @return a stream with the amount of jobs as specified in the limit parameter
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<MantaJob> getAllJobs(final int limit) throws IOException {
        if (limit < 0 || limit > MAX_RESULTS) {
            String msg = String.format("%d is invalid: must be between [1, %d]",
                    limit, MAX_RESULTS);
            throw new IllegalArgumentException(msg);
        }

        return getAllJobs("limit", String.valueOf(limit));
    }

    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} that match
     * the supplied job state from the Manta API. <strong>Make sure to close this stream
     * when you are done with otherwise the HTTP socket will remain open.</strong>
     *
     * @param state Manta job state
     * @return a stream with all of the jobs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<MantaJob> getJobsByState(final String state) throws IOException {
        return getAllJobs("state", state);
    }

    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} that match
     * the supplied name from the Manta API. <strong>Make sure to close this stream
     * when you are done with otherwise the HTTP socket will remain open.</strong>
     *
     * @param name job name to search for
     * @return a stream with all of the jobs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<MantaJob> getJobsByName(final String name) throws IOException {
        return getAllJobs("name", name);
    }

    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @param filterName filter name to filter request by (if none, default to null)
     * @param filter filter value to filter request by (if none, default to null)
     * @return a stream with all of the jobs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<MantaJob> getAllJobs(final String filterName,
                                       final String filter) throws IOException {

        return getAllJobIds(filterName, filter).map(id -> {
                if (id == null) {
                    return null;
                }

                try {
                    return getJob(id);
                } catch (IOException e) {
                    String msg = "Error filtering job object stream";
                    MantaJobException jobException = new MantaJobException(id, msg, e);

                    jobException.setContextValue("filterName", filterName);
                    jobException.setContextValue("filter", filter);

                    throw jobException;
                }
            });
    }

    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @return a stream with all of the job IDs (actually all that Manta will give us)
     */
    public Stream<UUID> getAllJobIds() {
        final String path = String.format("%s/jobs", home);

        final MantaDirectoryListingIterator itr = new MantaDirectoryListingIterator(this.url,
                path, httpHelper, MAX_RESULTS);

        danglingStreams.add(itr);

        Stream<Map<String, Object>> backingStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        itr, Spliterator.ORDERED | Spliterator.NONNULL), false);

        return backingStream.map(item -> {
            final String id = Objects.toString(item.get("name"));
            return UUID.fromString(id);
        });
    }

    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @param limit the maximum number of job ids to list 0-1024
     * @return a stream with the amount of jobs as specified in the limit parameter
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<UUID> getAllJobIds(final int limit) throws IOException {
        if (limit < 0 || limit > MAX_RESULTS) {
            String msg = String.format("%d is invalid: must be between [1, %d]",
                    limit, MAX_RESULTS);
            throw new IllegalArgumentException(msg);
        }
        return getAllJobIds("limit", String.valueOf(limit));
    }

    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} that match
     * the supplied job state from the Manta API. <strong>Make sure to close this stream
     * when you are done with otherwise the HTTP socket will remain open.</strong>
     *
     * @param state Manta job state
     * @return a stream with all of the job IDs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<UUID> getJobIdsByState(final String state) throws IOException {
        return getAllJobIds("state", state);
    }

    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} that matches
     * the supplied name from the Manta API. <strong>Make sure to close this stream
     * when you are done with otherwise the HTTP socket will remain open.</strong>
     *
     * @param name job name to search for
     * @return a stream with all of the job IDs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<UUID> getJobIdsByName(final String name) throws IOException {
        return getAllJobIds("name", name);
    }

    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @param filterName filter name to filter request by (if none, default to null)
     * @param filter filter value to filter request by (if none, default to null)
     * @return a stream with all of the job IDs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    private Stream<UUID> getAllJobIds(final String filterName,
                                      final String filter) throws IOException {
        final List<NameValuePair> params;

        if (filterName != null && filter != null) {
            NameValuePair pair = new BasicNameValuePair(filterName, filter);
            params = Collections.singletonList(pair);
        } else {
            params = Collections.emptyList();
        }

        final String path = formatPath(String.format("%s/jobs", home));
        final HttpGet get = connectionFactory.get(path, params);

        final HttpResponse response = httpHelper.executeRequest(get,
                "GET    {} response [{}] {} ");
        final ObjectMapper mapper = MantaObjectMapper.INSTANCE;
        final Stream<String> responseStream = responseAsStream(response);

        return responseStream.map(s -> {
            try {
                @SuppressWarnings("rawtypes")
                final Map jobDetails = mapper.readValue(s, Map.class);
                final Object value = jobDetails.get("name");

                if (value == null) {
                    return null;
                }

                return UUID.fromString(value.toString());
            } catch (IOException | IllegalArgumentException e) {
                String msg = "Error deserializing for job id stream";
                MantaJobException jobException = new MantaJobException(msg, e);
                HttpHelper.annotateContextedException(jobException, get, response);
                jobException.setContextValue("filterName", filterName);
                jobException.setContextValue("filter", filter);
                jobException.setContextValue("failedContent", s);

                throw jobException;
            }
        });
    }

    /**
     * <p>Returns the current "live" set of outputs from a job. Think of this
     * like tail -f. The objects are returned as a stream. The stream is
     * composed of a list of object names on Manta that contain the output
     * of the job.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     * @param jobId UUID of the Manta job
     * @return stream of object paths that refer to job output
     * @throws IOException thrown when we can't get a list of outputs over the network
     */
    public Stream<String> getJobOutputs(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Job id must not be null");
        String path = String.format("%s/jobs/%s/live/out", home, jobId);

        HttpGet get = connectionFactory.get(path);
        HttpResponse response = httpHelper.executeRequest(get,
                "GET    {} response [{}] {} ");
        return responseAsStream(response);
    }

    /**
     * <p>Returns a stream of {@link InputStream} implementations for each
     * output returned from the Manta API for a job.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     * @param jobId UUID of the Manta job
     * @return stream of each output's input stream
     * @throws IOException thrown when we can't get a list of outputs over the network
     */
    public Stream<MantaObjectInputStream> getJobOutputsAsStreams(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Job id must not be null");

        return getJobOutputs(jobId)
                .map(obj -> {
                    try {
                        return getAsInputStream(obj);
                    } catch (IOException e) {
                        String msg = "Error deserializing JSON output as InputStream";
                        MantaJobException jobException = new MantaJobException(jobId, msg, e);
                        jobException.setContextValue("output", obj);

                        throw jobException;
                    }
                });
    }

    /**
     * <p>Returns a stream of strings containing all of the
     * output returned from the Manta API for a job. Be careful, this method
     * is not memory-efficient.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     * @param jobId UUID of the Manta job
     * @return stream of each job output as a string
     * @throws IOException thrown when we can't get a list of outputs over the network
     */
    public Stream<String> getJobOutputsAsStrings(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Job id must not be null");

        return getJobOutputs(jobId)
                .map(obj -> {
                    try {
                        return getAsString(obj);
                    } catch (IOException e) {
                        String msg = "Error deserializing JSON output as string";
                        MantaJobException jobException = new MantaJobException(jobId, msg, e);
                        jobException.setContextValue("output", obj);

                        throw jobException;
                    }
                });
    }

    /**
     * <p>Returns the current "live" set of failures from a job. Think of this
     * like tail -f. The objects are returned as a stream. The stream is
     * composed of a list of object names on Manta that contain the output
     * of the job.</p>
     *
     * <p>Essentially, this method returns a stream of all of the Manta objects
     * whose jobs failed. There are no details about the nature of the failures
     * returned in this method.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     *
     * @param jobId UUID of the Manta job
     * @return stream of Manta object names whose jobs failed
     * @throws IOException thrown when we can't get a list of failures over the network
     */
    public Stream<String> getJobFailures(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Job id must not be null");

        String path = String.format("%s/jobs/%s/live/fail", home, jobId);

        final HttpGet get = connectionFactory.get(path);
        final HttpResponse response = httpHelper.executeRequest(get,
                "GET    {} response [{}] {} ");

        return responseAsStream(response);
    }

    /**
     * <p>Returns a list of failure details for each object in which a failure
     * occurred.</p>
     *
     * <p><strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong></p>
     *
     * @param jobId UUID of the Manta job
     * @return a stream of job error objects
     * @throws IOException thrown when we can't get a list of errors over the network
     */
    public Stream<MantaJobError> getJobErrors(final UUID jobId) throws IOException {
        Validate.notNull(jobId, "Job id must not be null");

        final String path = String.format("%s/jobs/%s/live/err", home, jobId);

        final HttpGet get = connectionFactory.get(path);
        final HttpResponse response = httpHelper.executeRequest(get,
                "GET    {} response [{}] {} ");
        final ObjectMapper mapper = MantaObjectMapper.INSTANCE;

        return responseAsStream(response)
                .map(err -> {
                    try {
                        return mapper.readValue(err, MantaJobError.class);
                    } catch (IOException e) {
                        String msg = "Error deserializing JSON job error output";
                        MantaJobException jobException = new MantaJobException(jobId, msg, e);
                        HttpHelper.annotateContextedException(jobException, get, response);
                        jobException.setContextValue("errText", err);

                        throw jobException;
                    }
                });
    }

    /**
     * Creates an instance of a {@link MantaJobBuilder} class that allows you
     * to fluently build Manta jobs.
     *
     * @return Manta job builder fluent interface class
     */
    public MantaJobBuilder jobBuilder() {
        return new MantaJobBuilder(this);
    }

    /**
     * Parses a HTTP response's content as a Java 8 stream of strings.
     *
     * @param response HTTP response object
     * @return stream of strings representing each line of the response
     * @throws IOException thrown when we can't access the response over the network
     */
    protected Stream<String> responseAsStream(final HttpResponse response)
            throws IOException {
        // This resource is closed using the onClose() lambda below
        final HttpEntity entity = response.getEntity();
        final Reader reader = new InputStreamReader(entity.getContent(),
                StandardCharsets.UTF_8);
        final BufferedReader br = new BufferedReader(reader);

        Stream<String> stream = br.lines().onClose(() -> {
            IOUtils.closeQuietly(br);

            if (response instanceof Closeable) {
                IOUtils.closeQuietly((Closeable)response);
            }
        });

        danglingStreams.add(stream);
        return stream;
    }

    /**
     * Flag indicating if the client is closed or is in the process of being
     * closed.
     *
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        if (this.closed) {
            return;
        }

        this.closed = true;

        final List<Exception> exceptions = new ArrayList<>();

        /* We explicitly close all streams that may have been opened when
         * this class (MantaClient) is closed. This helps to alleviate problems
         * where resources haven't been closed properly. In particular, this
         * is useful for the streamingIterator() method that returns an
         * iterator that must be closed after consumption. */
        for (AutoCloseable closeable : danglingStreams) {
            try {
                if (closeable == null) {
                    continue;
                }

                closeable.close();
            } catch (InterruptedException ie) {
                /* Do nothing, but we won't capture the interrupted exception
                 * because even if we are interrupted, we want to close all open
                 * resources. */
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        try {
            httpHelper.close();
        } catch (InterruptedException ie) {
            /* Do nothing, but we won't capture the interrupted exception
             * because even if we are interrupted, we want to close all open
             * resources. */
        } catch (Exception e) {
            exceptions.add(e);
        }

        try {
            connectionFactory.close();
        } catch (Exception e) {
            exceptions.add(e);
        }

        if (!exceptions.isEmpty()) {
            String msg = "At least one exception was thrown when performing close()";
            OnCloseAggregateException exception = new OnCloseAggregateException(msg);

            exceptions.forEach(exception::aggregateException);

            throw exception;
        }
    }

    /**
     * Closes the Manta client resource and logs any problems to the debug level
     * logger. No exceptions are thrown on failure.
     */
    public void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            LOG.debug("Error closing connection", e);
        }
    }

    /**
     * Closes the Manta client resource and logs any problems to the warn level
     * logger. No exceptions are thrown on failure.
     */
    public void closeWithWarning() {
        try {
            close();
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Error closing client", e);
            }
        }
    }
}
