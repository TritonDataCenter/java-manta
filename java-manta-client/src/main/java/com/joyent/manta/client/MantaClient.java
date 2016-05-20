/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.ObjectParser;
import com.joyent.http.signature.ThreadLocalSigner;
import com.joyent.http.signature.google.httpclient.RequestHttpSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaErrorCode;
import com.joyent.manta.exception.OnCloseAggregateException;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NoHttpResponseException;
import org.apache.http.entity.ContentType;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.joyent.manta.client.MantaUtils.formatPath;

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
     * The provider for http requests setup, metadata and request initialization.
     */
    private final HttpRequestFactoryProvider httpRequestFactoryProvider;

    /**
     * The standard http status code representing that the request was accepted.
     */
    private static final int HTTP_STATUSCODE_202_ACCEPTED = 202;

    /**
     * The content-type used to represent Manta link resources.
     */
    private static final String LINK_CONTENT_TYPE = "application/json; type=link";

    /**
     * The content-type used to represent Manta directory resources in http requests.
     */
    private static final String DIRECTORY_REQUEST_CONTENT_TYPE = "application/json; type=directory";

    /**
     * HTTP metadata headers that violate the Manta API contract.
     */
    private static final String[] ILLEGAL_METADATA_HEADERS = new String[]{
            HTTP.CONTENT_LEN, "Content-MD5", "Durability-Level"
    };

    /**
     * Maximum number of results to return for a directory listing.
     */
    private static final int MAX_RESULTS = 1024;

    /**
     * {@link ObjectParser} implementation used for parsing JSON HTTP responses.
     */
    private static final MantaObjectParser OBJECT_PARSER =
            new MantaObjectParser();


    /**
     * A string representation of the manta service endpoint URL.
     */
    private final String url;


    /**
     * The instance of the http signer used to sign the http requests.
     */
    private final RequestHttpSigner httpSigner;


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
    private final Set<WeakReference<? extends AutoCloseable>> danglingStreams
            = ConcurrentHashMap.newKeySet();

    /**
     * Creates a new instance of a Manta client.
     *
     * @param config The configuration context that provides all of the configuration values.
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final ConfigContext config) throws IOException {
        final String mantaURL = config.getMantaURL();
        final String account = config.getMantaUser();
        final String keyPath = config.getMantaKeyPath();
        final String fingerprint = config.getMantaKeyId();
        final int httpTimeout = config.getTimeout();
        final String privateKeyContent = config.getPrivateKeyContent();
        final String password = config.getPassword();

        if (account == null) {
            throw new IllegalArgumentException("Manta account name must be specified");
        }
        if (mantaURL == null) {
            throw new IllegalArgumentException("Manta URL must be specified");
        }
        if (httpTimeout < 0) {
            throw new IllegalArgumentException("Manta timeout must be 0 or greater");
        }
        if (privateKeyContent != null && keyPath != null) {
            throw new IllegalArgumentException("Private key content and key path can't be both set");
        } else if (privateKeyContent == null && keyPath == null) {
            throw new IllegalArgumentException("Manta key path or private key content must be specified");
        }

        if (config.noAuth() != null && config.noAuth()) {
            if (fingerprint == null) {
                throw new IllegalArgumentException("Manta key id must be specified");
            }
        }

        this.url = mantaURL;
        this.config = config;
        final KeyPair keyPair;
        final ThreadLocalSigner signer = new ThreadLocalSigner(!config.disableNativeSignatures());

        if (keyPath != null) {
            keyPair = signer.get().getKeyPair(new File(keyPath).toPath());
        } else {
            final char[] charPassword;

            if (password != null) {
                charPassword = password.toCharArray();
            } else {
                charPassword = null;
            }

            keyPair = signer.get().getKeyPair(privateKeyContent, charPassword);
        }

        this.httpSigner = new RequestHttpSigner(keyPair, account, fingerprint, signer);
        this.httpRequestFactoryProvider = new HttpRequestFactoryProvider(httpSigner,
                config);
        this.home = ConfigContext.deriveHomeDirectoryFromUser(account);
        this.httpHelper = new HttpHelper(mantaURL, httpRequestFactoryProvider.getRequestFactory());
    }


    /**
     * Deletes an object from Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If an HTTP status code {@literal > 300} is returned.
     */
    public void delete(final String path) throws IOException {
        LOG.debug("DELETE {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildDeleteRequest(genericUrl);

        httpHelper.executeAndCloseRequest(request, "DELETE {} response [{}] {} ", path);
    }


    /**
     * Recursively deletes an object in Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public void deleteRecursive(final String path) throws IOException {
        LOG.debug("DELETE {} [recursive]", path);

        if (isDirectoryEmpty(path)) {
            this.delete(path);
            LOG.debug("Finished deleting path {}", path);
            return;
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

        if (pathExists && isDirectoryEmpty(path)) {
            this.delete(path);
        } else if (pathExists) {
            try {
                final int waitTime = 400;
                Thread.sleep(waitTime);
                LOG.warn("First attempt to delete directory failed, retrying");
                // Re-attempt to delete the directory
                this.deleteRecursive(path);
            } catch (InterruptedException ie) {
                // We don't need to do anything, just exit
            }
        }

        LOG.debug("Finished deleting path {}", path);
    }


    /**
     * Get the metadata for a Manta object. The difference with this method vs head() is
     * that the request being made against the Manta API is done via a GET.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectResponse}.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse get(final String path) throws IOException {
        final HttpResponse response = httpHelper.httpGet(path);

        try {
            final MantaHttpHeaders headers = new MantaHttpHeaders(response.getHeaders());
            return new MantaObjectResponse(path, headers);
        } finally {
            try {
                response.disconnect();
            } catch (final IOException e) {
                LOG.warn("Problem disconnecting response resource", e);
            }
        }
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
        final HttpResponse response = httpHelper.httpGet(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders(response.getHeaders());
        final MantaObjectResponse metadata = new MantaObjectResponse(path, headers);

        if (metadata.isDirectory()) {
            final String msg = "Directories do not have data, so data streams "
                    + "from directories are not possible.";
            final MantaClientException exception = new MantaClientException(msg);
            exception.setContextValue("path", path);

            throw exception;
        }

        MantaObjectInputStream in = new MantaObjectInputStream(metadata, response);
        danglingStreams.add(new WeakReference<AutoCloseable>(in));

        return in;
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
            return MantaUtils.inputStreamToString(is);
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
            return MantaUtils.inputStreamToString(is, charsetName);
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
     * Get a Manta object's data as an NIO {@link SeekableByteChannel}. This method
     * allows you to stream data from the Manta storage service in a memory efficient
     * manner to your application. Unlike an {@link InputStream}, this will allow you
     * to stream data by moving between arbitrary position in the Manta object data.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param position The starting position (in number of bytes) to read from
     * @return seekable stream of object data
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaSeekableByteChannel getSeekableByteChannel(final String path,
                                                      final long position) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));

        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        return new MantaSeekableByteChannel(genericUrl, position,
                httpRequestFactory);
    }


    /**
     * Get a Manta object's data as an NIO {@link SeekableByteChannel}. This method
     * allows you to stream data from the Manta storage service in a memory efficient
     * manner to your application. Unlike an {@link InputStream}, this will allow you
     * to stream data by moving between arbitrary position in the Manta object data.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return seekable stream of object data
     * @throws IOException when there is a problem getting the object over the network
     */
    public MantaSeekableByteChannel getSeekableByteChannel(final String path) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));

        return new MantaSeekableByteChannel(genericUrl,
                httpRequestFactoryProvider.getRequestFactory());
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
        Objects.requireNonNull(expiresIn, "Duration must be present");
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
        Objects.requireNonNull(expires, "Expires must be present");

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
        Objects.requireNonNull(path, "Path must be present");

        final String fullPath = String.format("%s%s", this.url, formatPath(path));
        final URI request = URI.create(fullPath);

        return httpSigner.signURI(request, method, expiresEpochSeconds);
    }


    /**
     * Get the metadata associated with a Manta object.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectResponse}.
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse head(final String path) throws IOException {
        final HttpResponse response = httpHelper.httpHead(path);
        final MantaHttpHeaders headers = new MantaHttpHeaders(response.getHeaders());
        return new MantaObjectResponse(path, headers);
    }


    /**
     * Return a stream of the contents of a directory in Manta as an {@link Iterator}.
     *
     * @param path The fully qualified path of the directory.
     * @return A {@link Iterator} of {@link MantaObjectResponse} listing the contents of the directory.
     * @throws IOException thrown when there is a problem getting the listing over the network
     */
    public MantaDirectoryListingIterator streamingIterator(final String path) throws IOException {
        MantaDirectoryListingIterator itr = new MantaDirectoryListingIterator(
                this.url, path, httpHelper, MAX_RESULTS);
        danglingStreams.add(new WeakReference<AutoCloseable>(itr));
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

        Stream<Map<String, Object>> backingStream =
                StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                        itr, Spliterator.ORDERED | Spliterator.NONNULL), false);

        Stream<MantaObject> stream = backingStream.map(item -> {
            String name = Objects.toString(item.get("name"));
            String mtime = Objects.toString(item.get("mtime"));
            String type = Objects.toString(item.get("type"));
            Objects.requireNonNull(name, "File name must be present");
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

            return new MantaObjectResponse(objPath, headers);
        });

        danglingStreams.add(new WeakReference<AutoCloseable>(stream));

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
            throw new MantaClientException("The requested object was not a directory");
        }

        Long size = object.getHttpHeaders().getResultSetSize();

        if (size == null) {
            throw new MantaClientException("Expected result-set-size header to be present"
                + "but it was not part of the response");
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
            httpHelper.httpHead(path);
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
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaHttpHeaders headers) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = findOrDefaultContentType(headers,
                ContentType.APPLICATION_OCTET_STREAM.toString());

        final HttpContent content;

        if (source == null) {
            content = new EmptyContent();
        } else {
            content = new InputStreamContent(contentType, source);
        }

        return httpHelper.httpPut(path, headers, content, null);
    }

    /**
     * Puts an object into Manta.
     *
     * @param path     The path to the Manta object.
     * @param source   {@link InputStream} to copy object data from
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaMetadata metadata) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = ContentType.APPLICATION_OCTET_STREAM.toString();

        final HttpContent content;

        if (source == null) {
            content = new EmptyContent();
        } else {
            content = new InputStreamContent(contentType, source);
        }

        return httpHelper.httpPut(path, null, content, metadata);
    }


    /**
     * Puts an object into Manta.
     *
     * @param path     The path to the Manta object.
     * @param source   {@link InputStream} to copy object data from
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException                                     If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public MantaObjectResponse put(final String path,
                                   final InputStream source,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = findOrDefaultContentType(headers,
                ContentType.APPLICATION_OCTET_STREAM.toString());

        final HttpContent content;

        if (source == null) {
            content = new EmptyContent();
        } else {
            final InputStreamContent inputStreamContent =
                    new InputStreamContent(contentType, source);
            inputStreamContent.setRetrySupported(source.markSupported());
            content = inputStreamContent;
        }

        return httpHelper.httpPut(path, headers, content, metadata);
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
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param metadata optional user-supplied metadata for object
     * @param headers  optional HTTP headers to include when copying the object
     * @return A OutputStream that allows for directly uploading to Manta
     */
    public MantaObjectOutputStream putAsOutputStream(final String path,
                                                     final MantaHttpHeaders headers,
                                                     final MantaMetadata metadata) {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = findOrDefaultContentType(headers,
                ContentType.APPLICATION_OCTET_STREAM.toString());

        MantaObjectOutputStream stream = new MantaObjectOutputStream(path,
                this.httpHelper, headers, metadata, contentType);

        danglingStreams.add(new WeakReference<AutoCloseable>(stream));

        return stream;
    }


    /**
     * Copies the supplied {@link String} to a remote Manta object at the specified
     * path using the default JVM character encoding as a binary representation.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param string   string to copy
     * @param headers  optional HTTP headers to include when copying the object
     * @param metadata optional user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the object over the network
     */
    public MantaObjectResponse put(final String path,
                                   final String string,
                                   final MantaHttpHeaders headers,
                                   final MantaMetadata metadata) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = findOrDefaultContentType(headers,
                ContentType.APPLICATION_OCTET_STREAM.toString());

        final HttpContent content;

        if (string == null) {
            content = new EmptyContent();
        } else {
            content = new ByteArrayContent(contentType, string.getBytes());
        }

        return httpHelper.httpPut(path, headers, content, metadata);
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
        return put(path, string, null, null);
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
        Objects.requireNonNull(headers, "Headers must be present");

        final MantaMetadata metadata = new MantaMetadata(headers.metadataAsStrings());
        return putMetadata(path, headers, metadata);
    }


    /**
     * Appends the specified metadata to an existing Manta object using the
     * specified HTTP headers.
     *
     * @param path     The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @param headers  HTTP headers to include when copying the object
     * @param metadata user-supplied metadata for object
     * @return Manta response object
     * @throws IOException when there is a problem sending the metadata over the network
     */
    public MantaObjectResponse putMetadata(final String path,
                                           final MantaHttpHeaders headers,
                                           final MantaMetadata metadata)
            throws IOException {
        Objects.requireNonNull(headers, "Headers must be present");
        Objects.requireNonNull(metadata, "Metadata must be present");

        for (String header : ILLEGAL_METADATA_HEADERS) {
            if (headers.containsKey(header)) {
                String msg = String.format("Critical header [%s] can't be changed", header);
                throw new IllegalArgumentException(msg);
            }
        }

        headers.putAllMetadata(metadata);

        headers.setContentEncoding("chunked");
        HttpContent content = new EmptyContent();
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        return httpHelper.httpPut(genericUrl, headers, content, metadata);
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
     * @param path    The fully qualified path of the Manta directory.
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @return true when directory was created
     * @throws IOException                                     If an IO exception has occurred.
     * @throws MantaClientHttpResponseException                If a http status code {@literal > 300} is returned.
     */
    public boolean putDirectory(final String path, final MantaHttpHeaders headers)
            throws IOException {
        Objects.requireNonNull("PUT directory path must be present");

        LOG.debug("PUT    {} [directory]", path);
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, new EmptyContent());

        if (headers != null) {
            request.setHeaders(headers.asGoogleClientHttpHeaders());
        }

        request.getHeaders().setContentType(DIRECTORY_REQUEST_CONTENT_TYPE);
        request.setContent(new EmptyContent());

        HttpResponse res = httpHelper.executeAndCloseRequest(request,
                "PUT    {} response [{}] {} ", path);

        // When LastModified is set, the directory already exists
        return res.getHeaders().getLastModified() == null;
    }


    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @param recursive recursive create all of the directories specified in the path
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String path, final boolean recursive)
            throws IOException {
        putDirectory(path, recursive, null);
    }


    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @param recursive recursive create all of the directories specified in the path
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String path, final boolean recursive,
                             final MantaHttpHeaders headers)
            throws IOException {
        if (!recursive) {
            putDirectory(path, headers);
            return;
        }

        final String[] parts = path.split(SEPARATOR);
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
     * @param linkPath The fully qualified path of the new snaplink.
     * @param objectPath The fully qualified path of the object to link against.
     * @param headers Optional {@link MantaHttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putSnapLink(final String linkPath, final String objectPath,
                            final MantaHttpHeaders headers)
            throws IOException {
        LOG.debug("PUT    {} -> {} [snaplink]", objectPath, linkPath);
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(linkPath));
        final HttpContent content = new EmptyContent();
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, content);
        if (headers != null) {
            request.setHeaders(headers.asGoogleClientHttpHeaders());
        }

        request.getHeaders().setContentType(LINK_CONTENT_TYPE);
        request.getHeaders().setLocation(formatPath(objectPath));
        httpHelper.executeAndCloseRequest(request, "PUT    {} -> {} response [{}] {} ",
                objectPath, linkPath);
    }

    /**
     * Moves a file from one path to another path. This operation is not
     * transactional and will fail or produce inconsistent result if the source
     * or the destination is modified while the operation is in progress.
     *
     * @param source Original path to move from
     * @param destination Destination path to move to
     * @throws IOException thrown when something goes wrong
     */
    public void move(final String source, final String destination)
            throws IOException {
        LOG.debug("Moving [{}] to [{}]", source, destination);

        MantaObjectResponse entry = head(source);

        if (entry.isDirectory()) {
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

            try {
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
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }

            deleteRecursive(source);
        } else {
            putSnapLink(destination, source, null);
            delete(source);
        }
    }

    /*************************************************************************
     * Job Methods
     *************************************************************************/

    /**
     * Submits a new job to be executed. This call is not idempotent, so calling
     * it twice will create two jobs.
     *
     * @param job Object populated with details about the request job
     * @return id of the newly created job
     * @throws IOException thrown when there are problems creating the job over the network
     */
    public UUID createJob(final MantaJob job) throws IOException {
        Objects.requireNonNull(job, "Manta job must be present");

        String path = String.format("%s/jobs", home);
        ObjectMapper mapper = MantaObjectParser.MAPPER;
        byte[] json = mapper.writeValueAsBytes(job);

        HttpContent content = new ByteArrayContent(
                ContentType.APPLICATION_JSON.toString(),
                json);

        LOG.debug("POST   {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPostRequest(genericUrl, content);
        request.setContent(content);

        Function<HttpResponse, UUID> jobIdFunction = response -> {
            final String location = response.getHeaders().getLocation();
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
                return httpHelper.executeAndCloseRequest(request,
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
        Objects.requireNonNull(jobId, "Manta job id must be present");
        Objects.requireNonNull(inputs, "Inputs must be present");

        String contentType = "text/plain; charset=utf-8";
        HttpContent content = new StringIteratorHttpContent(inputs, contentType);

        processJobInputs(jobId, content);
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
        Objects.requireNonNull(jobId, "Manta job id must be present");
        Objects.requireNonNull(inputs, "Inputs must be present");

        String contentType = "text/plain; charset=utf-8";
        HttpContent content = new StringIteratorHttpContent(inputs, contentType);

        processJobInputs(jobId, content);
    }


    /**
     * Utility method for processing the addition of job inputs over HTTP.
     *
     * @param jobId UUID of the Manta job
     * @param content content object containing input objects
     * @throws IOException thrown when we are unable to add inputs over the network
     */
    protected void processJobInputs(final UUID jobId,
                                    final HttpContent content)
            throws IOException {

        String path = String.format("%s/jobs/%s/live/in", home, jobId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentEncoding("chunked");

        httpHelper.httpPost(path, content, headers);
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
        Objects.requireNonNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/in", home, jobId);

        HttpResponse response = httpHelper.httpGet(path);
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
        Objects.requireNonNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/in/end", home, jobId);

        final HttpContent content = new EmptyContent();
        HttpResponse response = httpHelper.httpPost(path, content);

        // We expect a return value of 202 when the cancel request was accepted
        return response.getStatusCode() == HTTP_STATUSCODE_202_ACCEPTED;
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
        Objects.requireNonNull(jobId, "Manta job id must not be null");
        String path = String.format("%s/jobs/%s/live/cancel",
                home, jobId);

        final HttpContent content = new EmptyContent();
        HttpResponse response = httpHelper.httpPost(path, content);

        // We expect a return value of 202 when the cancel request was accepted
        return response.getStatusCode() == HTTP_STATUSCODE_202_ACCEPTED;
    }

    /**
     * Gets the high-level job container object for a given id.
     * First, attempts to get the live status object and if it can't be
     * retrieved, then we get the archived status object.
     *
     * @param jobId UUID of the Manta job
     * @return Object representing the properties of the job
     * @throws IOException thrown when we can't get the job over the network
     */
    public MantaJob getJob(final UUID jobId) throws IOException {
        Objects.requireNonNull(jobId, "Manta job id must not be null");
        final String livePath = String.format("%s/jobs/%s/live/status",
                home, jobId);

        MantaJob job;
        HttpResponse response = null;

        try {
            response = httpHelper.httpGet(livePath, OBJECT_PARSER);
            job = response.parseAs(MantaJob.class);
        } catch (MantaClientHttpResponseException e) {
            // If we can't get the live status of the job, we try to get the archived
            // status of the job just like the CLI mjob utility.
            if (e.getServerCode().equals(MantaErrorCode.RESOURCE_NOT_FOUND_ERROR)) {
                final String archivePath = String.format("%s/jobs/%s/job.json",
                        home, jobId);
                response = httpHelper.httpGet(archivePath, OBJECT_PARSER);
                job = response.parseAs(MantaJob.class);
            } else {
                throw e;
            }
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }

        return job;
    }


    /**
     * Gets all of the Manta jobs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @return a stream with all of the jobs
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<MantaJob> getAllJobs() throws IOException {
        try {
            return getAllJobIds().map(id -> {
                if (id == null) {
                    return null;
                }

                try {
                    return getJob(id);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
        try {
            return getAllJobIds(filterName, filter).map(id -> {
                if (id == null) {
                    return null;
                }

                try {
                    return getJob(id);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }


    /**
     * Gets all of the Manta jobs' IDs as a real-time {@link Stream} from
     * the Manta API. <strong>Make sure to close this stream when you are done with
     * otherwise the HTTP socket will remain open.</strong>
     *
     * @return a stream with all of the job IDs (actually all that Manta will give us)
     * @throws IOException thrown when we can't get a list of jobs over the network
     */
    public Stream<UUID> getAllJobIds() throws IOException {
        final String path = String.format("%s/jobs", home);

        final MantaDirectoryListingIterator itr = new MantaDirectoryListingIterator(this.url,
                path, httpHelper, MAX_RESULTS);

        danglingStreams.add(new WeakReference<AutoCloseable>(itr));

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
        StringBuilder query = new StringBuilder("?");

        if (filterName != null && filter != null) {
            query.append(filterName).append("=").append(filter);
        }

        final String path = String.format("%s/jobs", home);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path)
                + query);

        final HttpResponse response = httpHelper.httpGet(genericUrl, null);
        final ObjectMapper mapper = MantaObjectParser.MAPPER;
        final Stream<String> responseStream = responseAsStream(response);

        try {
            return responseStream.map(s -> {
                try {
                    @SuppressWarnings("rawtypes")
                    final Map jobDetails = mapper.readValue(s, Map.class);
                    final Object value = jobDetails.get("name");

                    if (value == null) {
                        return null;
                    }

                    return UUID.fromString(value.toString());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
        Objects.requireNonNull(jobId, "Job id must be present");
        String path = String.format("%s/jobs/%s/live/out", home, jobId);

        HttpResponse response = httpHelper.httpGet(path);
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
        Objects.requireNonNull(jobId, "Job id must be present");

        try {
            return getJobOutputs(jobId)
                    .map(obj -> {
                        try {
                            return getAsInputStream(obj);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
        Objects.requireNonNull(jobId, "Job id must be present");

        try {
            return getJobOutputs(jobId)
                    .map(obj -> {
                        try {
                            return getAsString(obj);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
        Objects.requireNonNull(jobId, "Job id must be present");

        String path = String.format("%s/jobs/%s/live/fail", home, jobId);

        final HttpResponse response = httpHelper.httpGet(path);

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
        Objects.requireNonNull(jobId, "Job id must be present");

        final String path = String.format("%s/jobs/%s/live/err", home, jobId);

        final HttpResponse response = httpHelper.httpGet(path);
        final ObjectMapper mapper = MantaObjectParser.MAPPER;

        try {
            return responseAsStream(response)
                    .map(err -> {
                        try {
                            return mapper.readValue(err, MantaJobError.class);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
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
     * Finds the content type set in {@link MantaHttpHeaders} and returns that if it
     * is not null. Otherwise, it will return the specified default content type.
     *
     * @param headers headers to parse for content type
     * @param defaultContentType content type to default to
     * @return content type as string
     */
    protected static String findOrDefaultContentType(final MantaHttpHeaders headers,
                                                     final String defaultContentType) {
        final String contentType;

        if (headers == null || headers.getContentType() == null) {
            contentType = defaultContentType;
        } else {
            contentType = headers.getContentType();
        }

        return contentType;
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
        final Reader reader = new InputStreamReader(response.getContent());
        final BufferedReader br = new BufferedReader(reader);

        try {
            Stream<String> stream = br.lines().onClose(() -> {
                try {
                    br.close();
                    response.disconnect();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });

            danglingStreams.add(new WeakReference<AutoCloseable>(stream));
            return stream;
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }


    /**
     * Accessor for the HttpRequestFactoryProvider - used primarily for testing.
     *
     * @return provider instance
     */
    HttpRequestFactoryProvider getHttpRequestFactoryProvider() {
        return httpRequestFactoryProvider;
    }


    @Override
    public void close() throws Exception {
        final List<Exception> exceptions = new ArrayList<>();

        /* We explicitly close all streams that may have been opened when
         * this class (MantaClient) is closed. This helps to alleviate problems
         * where resources haven't been closed properly. In particular, this
         * is useful for the streamingIterator() method that returns an
         * iterator that must be closed after consumption. */
        for (WeakReference<? extends AutoCloseable> wr : danglingStreams) {
            try {
                AutoCloseable closeable = wr.get();

                if (closeable == null) {
                    continue;
                }

                closeable.close();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        try {
            this.httpRequestFactoryProvider.close();
        } catch (Exception e) {
            exceptions.add(e);
        }

        try {
            this.httpSigner.getSignerThreadLocal().clearAll();
        } catch (Exception e) {
            exceptions.add(e);
        }

        if (!exceptions.isEmpty()) {
            String msg = "At least one exception was thrown when performing close()";
            OnCloseAggregateException exception = new OnCloseAggregateException(msg);

            for (Exception e : exceptions) {
                exception.aggregateException(e);
            }

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
