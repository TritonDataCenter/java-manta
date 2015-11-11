/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.FileContent;
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
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.KeyPair;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Manta Http client.
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
     * The content-type used to represent Manta directory resources in http responses.
     */
    private static final String DIRECTORY_RESPONSE_CONTENT_TYPE = "application/x-json-stream; type=directory";

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
     * @return An instance of {@link com.joyent.manta.client.MantaClient}
     * @throws IOException If unable to instantiate the client.
     */
    public static MantaClient newInstance(final ConfigContext config) throws IOException {
        return newInstance(config.getMantaURL(),
                           config.getMantaUser(),
                           config.getMantaKeyPath(),
                           config.getMantaKeyId());
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param keyPath The path to the user rsa private key on disk.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @return An instance of {@link com.joyent.manta.client.MantaClient}
     * @throws IOException If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String keyPath,
                                          final String fingerPrint) throws IOException {
        return newInstance(url, login, keyPath, fingerPrint, DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT);
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param keyPath The path to the user rsa private key on disk.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @param timeout The http timeout in milliseconds.
     * @return An instance of {@link com.joyent.manta.client.MantaClient}
     * @throws IOException If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String keyPath,
                                          final String fingerPrint, final int timeout) throws IOException {
        LOG.debug(String.format("entering newInstance with url %s, login %s, keyPath %s, fingerPrint %s, timeout %s",
                url, login, keyPath, fingerPrint, timeout));
        return new MantaClient(url, login, keyPath, fingerPrint, timeout);
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param privateKeyContent The user's rsa private key as a string.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @param password The private key password (optional).
     * @return An instance of {@link com.joyent.manta.client.MantaClient}
     * @throws IOException If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url,
                                          final String login,
                                          final String privateKeyContent,
                                          final String fingerPrint,
                                          final char[] password) throws IOException {
        return newInstance(url,
                login,
                privateKeyContent,
                fingerPrint,
                password,
                DefaultsConfigContext.DEFAULT_HTTP_TIMEOUT
        );
    }


    /**
     * Creates a new instance of a Manta client.
     *
     * @param url The url of the Manta endpoint.
     * @param login The user login name.
     * @param privateKeyContent The private key as a string.
     * @param fingerPrint The fingerprint of the user rsa private key.
     * @param password The private key password (optional).
     * @param timeout The HTTP timeout in milliseconds.
     * @return An instance of {@link com.joyent.manta.client.MantaClient}
     * @throws IOException If unable to instantiate the client.
     */
    public static MantaClient newInstance(final String url, final String login, final String privateKeyContent,
                                          final String fingerPrint, final char[] password, final int timeout)
                                                  throws IOException {
        LOG.debug(String
                  .format("entering newInstance with url %s, login %s, privateKey ?, fingerPrint %s, password ?, "
                          + "timeout %d", url, login, fingerPrint, timeout));
        return new MantaClient(url, login, privateKeyContent, fingerPrint, password, timeout);
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
        this.httpRequestFactoryProvider = new HttpRequestFactoryProvider(httpSigner);
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
        this.httpRequestFactoryProvider = new HttpRequestFactoryProvider(httpSigner);
    }


    /**
     * Deletes an object from Manta.
     *
     * @param path The fully qualified path of the Manta object.
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If an HTTP status code {@literal > 300} is returned.
     */
    public void delete(final String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering delete with path %s", path));
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildDeleteRequest(genericUrl);
        // XXX: make an intercepter that sets these timeouts before each API call.
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);

        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ",
                                    response.getStatusCode(), response.getHeaders()));
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
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void deleteRecursive(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering deleteRecursive with path %s", path));
        Collection<MantaObject> objs;
        try {
            objs = this.listObjects(path);
        } catch (final MantaObjectException e) {
            this.delete(path);
            LOG.debug(String.format("finished deleting path %s", path));
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

        LOG.debug(String.format("finished deleting path %s", path));
    }


    /**
     * Get a Manta object.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObject get(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering get with path %s", path));
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildGetRequest(genericUrl);
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);

        HttpResponse response;
        try {
            response = request.execute();
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        final MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        if (response.getContentType().equals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
            mantaObject.setType("directory");
        }
        mantaObject.setDataInputStream(response.getContent());
        mantaObject.setHttpHeaders(response.getHeaders());
        mantaObject.setContentLength(response.getHeaders().getContentLength());
        mantaObject.setEtag(response.getHeaders().getETag());
        mantaObject.setMtime(response.getHeaders().getLastModified());

        LOG.debug(String.format("got response code %s, MantaObject %s, header %s ", response.getStatusCode(),
                                mantaObject, response.getHeaders()));
        return mantaObject;
    }

    /**
     * Get the metadata associated with a Manta object.
     *
     * @param path The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException If an IO exception has occurred.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public MantaObject head(final String path) throws MantaCryptoException, MantaClientHttpResponseException,
    IOException {
        LOG.debug(String.format("entering get with path %s", path));
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildHeadRequest(genericUrl);
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);

        HttpResponse response;
        try {
            response = request.execute();
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        final MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        if (response.getContentType().equals(DIRECTORY_RESPONSE_CONTENT_TYPE)) {
            mantaObject.setType("directory");
        }
        mantaObject.setContentLength(response.getHeaders().getContentLength());
        mantaObject.setEtag(response.getHeaders().getETag());
        mantaObject.setMtime(response.getHeaders().getLastModified());
        return mantaObject;
    }


    /**
     * Return the contents of a directory in Manta.
     *
     * @param path The fully qualified path of the directory.
     * @return A {@link Collection} of {@link MantaObject} listing the contents of the directory.
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaObjectException If the path isn't a directory
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public Collection<MantaObject> listObjects(final String path) throws MantaCryptoException, MantaObjectException,
            MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering listDirectory with directory %s", path));
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildGetRequest(genericUrl);
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);

        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));

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
     * Creates a list of {@link MantaObject}s based on the HTTP response from Manta.
     * @param path The fully qualified path of the directory.
     * @param content The content of the response as a Reader.
     * @param parser Deserializer implementation that takes the raw content and turns it into a {@link MantaObject}
     * @return List of {@link MantaObject}s for a given directory
     * @throws IOException If an IO exception has occurred.
     */
    protected static List<MantaObject> buildObjects(final String path,
                                                    final BufferedReader content,
                                                    final ObjectParser parser) throws IOException {
        final ArrayList<MantaObject> objs = new ArrayList<>();
        String line;
        StringBuilder myPath = new StringBuilder(path);
        while ((line = content.readLine()) != null) {
            final MantaObject obj = parser.parseAndClose(new StringReader(line), MantaObject.class);
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
     * @param object The stored Manta object. This must contain the fully qualified path of the object, along with
     *               optional data either stored as an {@link java.io.InputStream}, {@link java.io.File},
     *               or {@link java.lang.String}.
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void put(final MantaObject object)
            throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering put with manta object %s, headers %s", object, object.getHttpHeaders()));
        String contentType = null;
        if (object.getHttpHeaders() != null) {
            contentType = object.getHttpHeaders().getContentType();
        }
        HttpContent content;
        if (object.getDataInputStream() != null) {
            content = new InputStreamContent(contentType, object.getDataInputStream());
        } else if (object.getDataInputFile() != null) {
            content = new FileContent(contentType, object.getDataInputFile());
        } else if (object.getDataInputString() != null) {
            content = new ByteArrayContent(contentType, object.getDataInputString().getBytes("UTF-8"));
        } else {
            content = new EmptyContent();
        }
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(object.getPath()));

        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, content);
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);
        if (object.getHttpHeaders() != null) {
            request.setHeaders(object.getHttpHeaders());
        }

        HttpResponse response = null;
        try {
            LOG.debug(String.format("sending request %s", request));
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    /**
     * Creates a directory in Manta.
     *
     * @param path The fully qualified path of the Manta directory.
     * @param headers Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException If an IO exception has occured.
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putDirectory(final String path, final HttpHeaders headers)
            throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        if (path == null) {
            throw new IllegalArgumentException("PUT directory path can't be null");
        }

        LOG.debug(String.format("entering putDirectory with directory %s", path));
        final GenericUrl genericUrl = new GenericUrl(this.url + formatPath(path));
        final HttpRequestFactory httpRequestFactory = httpRequestFactoryProvider.getRequestFactory();
        final HttpRequest request = httpRequestFactory.buildPutRequest(genericUrl, new EmptyContent());
        request.setReadTimeout(this.httpTimeout);
        request.setConnectTimeout(this.httpTimeout);
        if (headers != null) {
            request.setHeaders(headers);
        }

        request.getHeaders().setContentType(DIRECTORY_REQUEST_CONTENT_TYPE);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
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
     * @throws MantaCryptoException If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException If a http status code {@literal > 300} is returned.
     */
    public void putSnapLink(final String linkPath, final String objectPath, final HttpHeaders headers)
            throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering putLink with link %s, path %s", linkPath, objectPath));
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
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(),
                                    response.getHeaders()));
        } catch (final HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }


    /**
     * Format the path according to RFC3986.
     *
     * @param path the raw path string.
     * @return the URI formatted string with the exception of '/' which is special in manta.
     * @throws UnsupportedEncodingException If UTF-8 is not supported on this system.
     */
    private static String formatPath(final String path) throws UnsupportedEncodingException {
        // first split the path by slashes.
        final String[] elements = path.split("/");
        final StringBuilder encodedPath = new StringBuilder();
        for (final String string : elements) {
            if (string.equals("")) {
                continue;
            }
            encodedPath.append("/").append(URLEncoder.encode(string, "UTF-8"));
        }
        return encodedPath.toString();
    }


    @Override
    public void close() throws Exception {
        this.httpRequestFactoryProvider.close();
    }
}
