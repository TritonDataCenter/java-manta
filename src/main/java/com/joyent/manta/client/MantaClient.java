/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.ObjectParser;
import com.joyent.http.signature.HttpSignerUtils;
import com.joyent.http.signature.google.httpclient.HttpSigner;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.DefaultsConfigContext;
import com.joyent.manta.exception.MantaClientException;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaObjectException;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.joyent.manta.client.MantaUtils.formatPath;

/**
 * Manta client object that allows for doing CRUD operations against the Manta HTTP
 * API. Using this client you can retrieve implementations of {@link MantaObject}.
 *
 * @author Yunong Xiao
 */
public class MantaClient implements AutoCloseable {

    /**
     * The static logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaClient.class);

    /**
     * The provide for http requests setup, metadata and request initialization.
     */
    private final HttpRequestFactoryProvider httpRequestFactoryProvider;

    /**
     * The standard http status code representing that the resource could not be found.
     */
    private static final int HTTP_STATUSCODE_404_NOT_FOUND = 404;

    /**
     * The content-type used to represent Manta link resources.
     */
    private static final String LINK_CONTENT_TYPE = "application/json; type=link";

    /**
     * The content-type used to represent Manta directory resources in http requests.
     */
    private static final String DIRECTORY_REQUEST_CONTENT_TYPE = "application/json; type=directory";

    /**
     * A string representation of the manta service endpoint URL.
     */
    private final String url;


    /**
     * The instance of the http signer used to sign the http requests.
     */
    private final HttpSigner httpSigner;


    /**
     * The timeout (in milliseconds) for accessing the Manta service.
     */
    private final int httpTimeout;

    /**
     * Creates a new instance of a Manta client.
     *
     * @param config The configuration context that provides all of the configuration values.
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final ConfigContext config) throws IOException {
        this(config.getMantaURL(), config.getMantaUser(), config.getMantaKeyPath(),
             config.getMantaKeyId());
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param keyPath The path to the user rsa private key on disk.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final String url, final String login, final String keyPath,
                              final String fingerPrint) throws IOException {
        this(url, login, keyPath, fingerPrint, DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param privateKeyContent The user's rsa private key as a string.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @param password The private key password (optional).
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final String url,
                       final String login,
                       final String privateKeyContent,
                       final String fingerPrint,
                       final char[] password) throws IOException {
        this(url, login, privateKeyContent, fingerPrint, password,
             DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
    }


    /**
     * Instantiates a new Manta client instance.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param keyPath The path to the user rsa private key on disk.
     * @param fingerprint The fingerprint of the user rsa private key.
     * @param httpTimeout The HTTP timeout in milliseconds.
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final String url,
                       final String login,
                       final String keyPath,
                       final String fingerprint,
                       final int httpTimeout) throws IOException {
        if (login == null) {
            throw new IllegalArgumentException("Manta username must be specified");
        }
        if (url == null) {
            throw new IllegalArgumentException("Manta URL must be specified");
        }
        if (keyPath == null) {
            throw new IllegalArgumentException("Manta key path must be specified");
        }
        if (fingerprint == null) {
            throw new IllegalArgumentException("Manta key id must be specified");
        }
        if (httpTimeout < 0) {
            throw new IllegalArgumentException("Manta timeout must be 0 or greater");
        }

        this.url = url;
        KeyPair keyPair = HttpSignerUtils.getKeyPair(new File(keyPath).toPath());
        this.httpSigner = new HttpSigner(keyPair, login, fingerprint);
        this.httpRequestFactoryProvider = new HttpRequestFactoryProvider(httpSigner,
                httpTimeout);
        this.httpTimeout = httpTimeout;
    }


    /**
     * Instantiates a new Manta client instance.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param privateKeyContent The private key as a string.
     * @param fingerprint The name of the key.
     * @param password The private key password (optional).
     * @param httpTimeout The HTTP timeout in milliseconds.
     * @throws IOException If unable to instantiate the client.
     */
    public MantaClient(final String url,
                       final String login,
                       final String privateKeyContent,
                       final String fingerprint,
                       final char[] password,
                       final int httpTimeout) throws IOException {
        if (login == null) {
            throw new IllegalArgumentException("Manta username must be specified");
        }
        if (url == null) {
            throw new IllegalArgumentException("Manta URL must be specified");
        }
        if (fingerprint == null) {
            throw new IllegalArgumentException("Manta fingerprint must be specified");
        }
        if (password == null) {
            throw new IllegalArgumentException("Manta key password must be specified");
        }
        if (httpTimeout < 0) {
            throw new IllegalArgumentException("Manta timeout must be 0 or greater");
        }

        this.url = url;
        KeyPair keyPair = HttpSignerUtils.getKeyPair(privateKeyContent, password);
        this.httpSigner = new HttpSigner(keyPair, login, fingerprint);
        this.httpTimeout = httpTimeout;
        this.httpRequestFactoryProvider = new HttpRequestFactoryProvider(httpSigner,
                httpTimeout);
    }


    /**
     * Deletes an object from Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If an HTTP status code {@literal > 300} is returned.
     */
    public void delete(final String path) throws IOException {
        LOG.debug("DELETE {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildDeleteRequest(genericUrl);

        HttpResponse response = null;
        try {
            response = request.execute();
            if (LOG.isDebugEnabled()) {
                LOG.debug("DELETE {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    /**
     * Recursively deletes an object in Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void deleteRecursive(final String path) throws IOException {
        LOG.debug("DELETE {} [recursive]", path);
        Collection<MantaObject> objs;
        try {
            objs = this.listObjects(path);
        } catch (final MantaObjectException e) {
            this.delete(path);
            LOG.debug("Finished deleting path {}", path);
            return;
        }
        for (final MantaObject mantaObject : objs) {
            if (mantaObject.isDirectory()) {
                this.deleteRecursive(mantaObject.getPath());
            } else {
                this.delete(mantaObject.getPath());
            }
        }

        try {
            this.delete(path);
        } catch (MantaClientHttpResponseException e) {
            if (e.getStatusCode() == HTTP_STATUSCODE_404_NOT_FOUND && LOG.isDebugEnabled()) {
                LOG.debug("Couldn't delete object because it doesn't exist", e);
            } else {
                throw e;
            }
        }

        LOG.debug("Finished deleting path {}", path);
    }


    /**
     * Get a Manta object.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectMetadata}.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObjectMetadata get(final String path) throws IOException {
        final HttpResponse response = httpGet(path);
        return new MantaObjectMetadata(path, response.getHeaders());
    }

    public MantaObjectInputStream getInputStream(final String path) throws IOException {
        final HttpResponse response = httpGet(path);
        final MantaObjectMetadata metadata = new MantaObjectMetadata(path, response.getHeaders());

        if (metadata.isDirectory()) {
            String msg = String.format("Directories do not have data, so "
                    + "data streams from them doesn't work. Path requested: %s",
                    path);
            throw new MantaClientException(msg);
        }

        return new MantaObjectInputStream(metadata, response);
    }

    public String getAsString(final String path) throws IOException {
        try (InputStream is = getInputStream(path)) {
            return MantaUtils.inputStreamToString(is);
        }
    }

    public String getAsString(final String path, final String charsetName) throws IOException {
        try (InputStream is = getInputStream(path)) {
            return MantaUtils.inputStreamToString(is, charsetName);
        }
    }

    public Path getToTempPath(final String path) throws IOException {
        try (InputStream is = getInputStream(path)) {
            final Path temp = Files.createTempFile("manta-object", "tmp");

            Files.copy(is, temp, StandardCopyOption.REPLACE_EXISTING);

            return temp;
        }
    }

    public File getToTempFile(final String path) throws IOException {
        return getToTempPath(path).toFile();
    }

    public SeekableByteChannel getSeekableByteChannel(final String path) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));

        return new MantaSeekableByteChannel(genericUrl,
                httpRequestFactoryProvider.getRequestFactory());
    }

    public SeekableByteChannel getSeekableByteChannel(final String path,
                                                      final long position) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));

        return new MantaSeekableByteChannel(genericUrl, position,
                httpRequestFactoryProvider.getRequestFactory());
    }

    protected HttpResponse httpGet(final String path) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        LOG.debug("GET    {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildGetRequest(genericUrl);

        final HttpResponse response;

        try {
            response = request.execute();

            if (LOG.isDebugEnabled()) {
                LOG.debug("GET    {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }

        return response;
    }

    /**
     * Get the metadata associated with a Manta object.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObjectMetadata}.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObjectMetadata head(final String path) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        LOG.debug("HEAD   {}", path);

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildHeadRequest(genericUrl);

        HttpResponse response;
        try {
            response = request.execute();

            if (LOG.isDebugEnabled()) {
                LOG.debug("HEAD   {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }

        return new MantaObjectMetadata(path, response.getHeaders());
    }


    /**
     * Return the contents of a directory in Manta.
     *
     * @param path The fully qualified path of the directory.
     * @return A {@link Collection} of {@link MantaObjectMetadata} listing the contents of the directory.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaObjectException If the path isn't a directory
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public Collection<MantaObject> listObjects(final String path) throws IOException {
        LOG.debug("GET    {} [list]", path);
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildGetRequest(genericUrl);

        HttpResponse response = null;
        try {
            response = request.execute();

            if (LOG.isDebugEnabled()) {
                LOG.debug("GET    {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }

            if (!response.getContentType().equals(MantaObject.DIRECTORY_HEADER)) {
                throw new MantaObjectException("Object is not a directory");
            }

            final BufferedReader br = new BufferedReader(new InputStreamReader(response.getContent()));
            final ObjectParser parser = request.getParser();
            return buildObjects(path, br, parser);
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    /**
     * Creates a list of {@link MantaObjectMetadata}s based on the HTTP response from Manta.
     * @param path The fully qualified path of the directory.
     * @param content The content of the response as a Reader.
     * @param parser Deserializer implementation that takes the raw content
     *               and turns it into a {@link MantaObjectMetadata}
     * @return List of {@link MantaObjectMetadata}s for a given directory
     * @throws IOException If an IO exception has occurred.
     */
    protected static List<MantaObject> buildObjects(final String path,
                                                    final BufferedReader content,
                                                    final ObjectParser parser) throws IOException {
        final ArrayList<MantaObject> objs = new ArrayList<>();
        String line;
        StringBuilder myPath = new StringBuilder(path);
        while ((line = content.readLine()) != null) {
            final MantaObjectMetadata obj = parser.parseAndClose(new StringReader(line), MantaObjectMetadata.class);
            // need to prefix the obj name with the fully qualified path, since Manta only returns
            // the explicit name of the object.
            if (!MantaUtils.endsWith(myPath, '/')) {
                myPath.append('/');
            }

            obj.setPath(myPath + obj.getPath());
            objs.add(obj);
        }
        return objs;
    }


    /**
     * Puts an object into Manta.
     *
     * @param path The path to the Manta object.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void put(final String path,
                    final InputStream source,
                    final HttpHeaders headers) throws IOException {
        Objects.requireNonNull(path, "Path must not be null");

        final String contentType = findOrDefaultContentType(headers, HTTP.OCTET_STREAM_TYPE);

        final HttpContent content;

        if (source == null) {
            content = new EmptyContent();
        } else {
            content = new InputStreamContent(contentType, source);
        }

        httpPut(path, headers, content);
    }

    protected HttpResponse httpPut(final String path,
                                   final HttpHeaders headers,
                                   final HttpContent content) throws IOException {
        LOG.debug("PUT    {}", path);

        final HttpHeaders httpHeaders;

        if (headers == null) {
            httpHeaders = new HttpHeaders();
        } else {
            httpHeaders = headers;
        }

        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));

        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, content);

        request.setHeaders(httpHeaders);

        HttpResponse response = null;
        try {
            response = request.execute();
            if (LOG.isDebugEnabled()) {
                LOG.debug("PUT    {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }

        return response;
    }


    public void put(final String path,
                    final InputStream source) throws IOException {
        put(path, source, null);
    }


    public void put(final String path,
                    final String string,
                    final HttpHeaders headers) throws IOException {
        try (InputStream is = new ByteArrayInputStream(string.getBytes())) {
            put(path, is, headers);
        }
    }

    public void put(final String path,
                    final String string) throws IOException {
        put(path, string, null);
    }


    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @param headers Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String path, final HttpHeaders headers)
            throws IOException {
        if (path == null) {
            throw new IllegalArgumentException("PUT directory path can't be null");
        }

        LOG.debug("PUT    {} [directory]", path);
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, new EmptyContent());

        if (headers != null) {
            request.setHeaders(headers);
        }

        request.getHeaders().setContentType(DIRECTORY_REQUEST_CONTENT_TYPE);
        HttpResponse response = null;
        try {
            response = request.execute();
            if (LOG.isDebugEnabled()) {
                LOG.debug("PUT    {} response [{}] {} ", path, response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    /**
     * Create a Manta snaplink.
     *
     * @param linkPath The fully qualified path of the new snaplink.
     * @param objectPath The fully qualified path of the object to link against.
     * @param headers Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occurred.
     * @throws com.joyent.manta.exception.MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putSnapLink(final String linkPath, final String objectPath,
                            final HttpHeaders headers)
            throws IOException {
        LOG.debug("PUT    {} -> {} [snaplink]", objectPath, linkPath);
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(linkPath));
        final HttpContent content = new EmptyContent();
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, content);
        if (headers != null) {
            request.setHeaders(headers);
        }

        request.getHeaders().setContentType(LINK_CONTENT_TYPE);
        request.getHeaders().setLocation(formatPath(objectPath));
        HttpResponse response = null;
        try {
            response = request.execute();
            if (LOG.isDebugEnabled()) {
                LOG.debug("PUT    {} -> {} response [{}] {} ", objectPath, linkPath,
                        response.getStatusCode(),
                        response.getStatusMessage());
            }
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    protected static String findOrDefaultContentType(final HttpHeaders headers,
                                                     final String defaultContentType) {
        final String contentType;

        if (headers == null || headers.getContentType() == null) {
            contentType = defaultContentType;
        } else {
            contentType = headers.getContentType();
        }

        return contentType;
    }


    @Override
    public void close() throws Exception {
        this.httpRequestFactoryProvider.close();
    }
}
