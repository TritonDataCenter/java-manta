/**
 * Copyright (c) 2014, Joyent, Inc. All rights reserved.
 */
package com.joyent.manta.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.joyent.manta.client.crypto.HttpSigner;
import com.joyent.manta.exception.MantaClientHttpResponseException;
import com.joyent.manta.exception.MantaCryptoException;
import com.joyent.manta.exception.MantaObjectException;

/**
 * Manta Http client.
 * 
 * @author Yunong Xiao
 */
public class MantaClient {
    private static final Log LOG = LogFactory.getLog(MantaClient.class);

    private static final JsonFactory JSON_FACTORY = new JacksonFactory();
    private static final HttpRequestFactory HTTP_REQUEST_FACTORY = new NetHttpTransport()
            .createRequestFactory(new HttpRequestInitializer() {

                @Override
                public void initialize(HttpRequest request) throws IOException {
                    request.setParser(new JsonObjectParser(JSON_FACTORY));
                }
            });

    private static final String LINK_CONTENT_TYPE = "application/json; type=link";
    private static final String DIRECTORY_CONTENT_TYPE = "application/json; type=directory";

    private static final int DEFAULT_HTTP_TIMEOUT = 20 * 1000;

    /**
     * Creates a new instance of a Manta client.
     * 
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param keyPath
     *            The path to the user rsa private key on disk.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     */
    public static MantaClient newInstance(String url, String login, String keyPath, String fingerPrint)
            throws IOException {
        return newInstance(url, login, keyPath, fingerPrint, DEFAULT_HTTP_TIMEOUT);
    }

    /**
     * Creates a new instance of a Manta client.
     * 
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param keyPath
     *            The path to the user rsa private key on disk.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param timeout
     *            The http timeout in milliseconds.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     */
    public static MantaClient newInstance(String url, String login, String keyPath, String fingerPrint, int timeout)
            throws IOException {
        LOG.debug(String.format("entering newInstance with url %s, login %s, keyPath %s, fingerPrint %s, timeout %s",
                                url, login, keyPath, fingerPrint, timeout));
        return new MantaClient(url, login, keyPath, fingerPrint, timeout);
    }

    /**
     * Creates a new instance of a Manta client
     * 
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param privateKeyContent
     *            The user's rsa private key as a string.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param password
     *            The private key password (optional).
     * @return An instance of {@link MantaClient}
     * @throws IOException
     */
    public static MantaClient newInstance(String url, String login, String privateKeyContent, String fingerPrint,
                                          char[] password) throws IOException {
        return newInstance(url, login, privateKeyContent, fingerPrint, password, DEFAULT_HTTP_TIMEOUT);
    }

    /**
     * Creates a new instance of a Manta client
     * 
     * @param url
     *            The url of the Manta endpoint.
     * @param login
     *            The user login name.
     * @param privateKeyContent
     *            The private key as a string.
     * @param fingerPrint
     *            The fingerprint of the user rsa private key.
     * @param password
     *            The private key password (optional).
     * @param timeout
     *            The HTTP timeout in milliseconds.
     * @return An instance of {@link MantaClient}
     * @throws IOException
     */
    public static MantaClient newInstance(String url, String login, String privateKeyContent, String fingerPrint,
                                          char[] password, int timeout) throws IOException {
        LOG.debug(String
                .format("entering newInstance with url %s, login %s, privateKey ?, fingerPrint %s, password ?, "
                    + "timeout %d", url, login, fingerPrint, timeout));
        return new MantaClient(url, login, privateKeyContent, fingerPrint, password, timeout);
    }

    private final String url_;

    private final HttpSigner httpSigner_;

    private final int httpTimeout_;

    private MantaClient(String url, String login, String keyPath, String fingerPrint, int httpTimeout)
                                                                                                      throws IOException {
        url_ = url;
        httpSigner_ = HttpSigner.newInstance(keyPath, fingerPrint, login);
        httpTimeout_ = httpTimeout;
    }

    private MantaClient(String url, String login, String privateKeyContent, String keyName, char[] password,
                        int httpTimeout) throws IOException {
        url_ = url;
        httpSigner_ = HttpSigner.newInstance(privateKeyContent, keyName, password, login);
        httpTimeout_ = httpTimeout;
    }

    /**
     * Deletes an object from Manta.
     * 
     * @param path
     *            The fully qualified path of the Manta object.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If an HTTP status code > 300 is returned.
     */
    public void delete(String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering delete with path %s", path));
        GenericUrl url = new GenericUrl(url_ + formatPath(path));
        HttpRequest request = HTTP_REQUEST_FACTORY.buildDeleteRequest(url);
        // TODO: make an intercepter that sets these timeouts before each API call.
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(), response.getHeaders()));
        } catch (HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                try {
                    response.disconnect();
                } catch (HttpResponseException e) {
                    throw new MantaClientHttpResponseException(e);
                }
            }
        }
    }

    /**
     * Recursively deletes an object in Manta.
     * 
     * @param path
     *            The fully qualified path of the Manta object.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void deleteRecursive(String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering deleteRecursive with path %s", path));
        Collection<MantaObject> objs = null;
        try {
            objs = listObjects(path);
        } catch (MantaObjectException e) {
            delete(path);
            LOG.debug(String.format("finished deleting path %s", path));
            return;
        }
        for (MantaObject mantaObject : objs) {
            if (mantaObject.isDirectory()) {
                deleteRecursive(mantaObject.getPath());
            } else {
                delete(mantaObject.getPath());
            }
        }
        delete(path);
        LOG.debug(String.format("finished deleting path %s", path));
    }

    /**
     * Get a Manta object.
     * 
     * @param path
     *            The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public MantaObject get(String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering get with path %s", path));
        GenericUrl url = new GenericUrl(url_ + formatPath(path));
        HttpRequest request = HTTP_REQUEST_FACTORY.buildGetRequest(url);
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
        } catch (HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        mantaObject.setDataInputStream(response.getContent());
        mantaObject.setHttpHeaders(response.getHeaders());
        LOG.debug(String.format("got response code %s, MantaObject %s, header %s ", response.getStatusCode(),
                                mantaObject, response.getHeaders()));
        return mantaObject;
    }

    /**
     * Get the metadata associated with a Manta object.
     * 
     * @param path
     *            The fully qualified path of the object. i.e. /user/stor/foo/bar/baz
     * @return The {@link MantaObject}.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public MantaObject head(String path) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering get with path %s", path));
        GenericUrl url = new GenericUrl(url_ + formatPath(path));
        HttpRequest request = HTTP_REQUEST_FACTORY.buildHeadRequest(url);
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
        } catch (HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        }
        MantaObject mantaObject = new MantaObject(path, response.getHeaders());
        LOG.debug(String.format("got response code %s, MantaObject %s, header %s", response.getStatusCode(),
                                mantaObject, response.getHeaders()));
        return mantaObject;
    }

    /**
     * Return the contents of a directory in Manta.
     * 
     * @param path
     *            The fully qualified path of the directory.
     * @return A {@link Collection} of {@link MantaObject} listing the contents of the directory.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaObjectException
     *             If the path isn't a directory
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public Collection<MantaObject> listObjects(String path) throws MantaCryptoException, MantaObjectException,
            MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering listDirectory with directory %s", path));
        GenericUrl url = new GenericUrl(url_ + formatPath(path));
        HttpRequest request = HTTP_REQUEST_FACTORY.buildGetRequest(url);
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(), response.getHeaders()));

            if (!response.getContentType().equals(MantaObject.DIRECTORY_HEADER)) {
                throw new MantaObjectException("Object is not a directory");
            }

            ArrayList<MantaObject> objs = new ArrayList<MantaObject>();
            BufferedReader br = new BufferedReader(new InputStreamReader(response.getContent()));
            String line;
            while ((line = br.readLine()) != null) {
                MantaObject obj = request.getParser().parseAndClose(new StringReader(line), MantaObject.class);
                // need to prefix the obj name with the fully qualified path, since Manta only returns
                // the explicit name of the object.
                if (!path.endsWith("/")) {
                    path += "/";
                }
                obj.setPath(path + obj.getPath());
                objs.add(obj);
            }
            return objs;
        } catch (HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Puts an object into Manta.
     * 
     * @param object
     *            The stored Manta object. This must contain the fully qualified path of the object, along with optional
     *            data either stored as an {@link InputStream}, {@link File}, or {@link String}.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void put(MantaObject object) throws MantaCryptoException, MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering put with manta object %s, headers %s", object, object.getHttpHeaders()));
        String contentType = null;
        if (object.getHttpHeaders() != null) {
            contentType = object.getHttpHeaders().getContentType();
        }
        HttpContent content = null;
        if (object.getDataInputStream() != null) {
            content = new InputStreamContent(contentType, object.getDataInputStream());
        } else if (object.getDataInputFile() != null) {
            content = new FileContent(contentType, object.getDataInputFile());
        } else if (object.getDataInputString() != null) {
            content = new ByteArrayContent(contentType, object.getDataInputString().getBytes("UTF-8"));
        } else {
            content = new EmptyContent();
        }
        GenericUrl url = new GenericUrl(url_ + formatPath(object.getPath()));

        HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, content);
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        if (object.getHttpHeaders() != null) {
            request.setHeaders(object.getHttpHeaders());
        }
        httpSigner_.signRequest(request);
        HttpResponse response = null;
        try {
            LOG.debug(String.format("sending request %s", request));
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(), response.getHeaders()));
        } catch (HttpResponseException e) {
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
     * @param path
     *            The fully qualified path of the Manta directory.
     * @param headers
     *            Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void putDirectory(String path, HttpHeaders headers) throws MantaCryptoException,
            MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering putDirectory with directory %s", path));
        GenericUrl url = new GenericUrl(url_ + formatPath(path));
        HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, new EmptyContent());
        request.setReadTimeout(httpTimeout_);
        request.setConnectTimeout(httpTimeout_);
        if (headers != null) {
            request.setHeaders(headers);
        }
        httpSigner_.signRequest(request);
        request.getHeaders().setContentType(DIRECTORY_CONTENT_TYPE);
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(), response.getHeaders()));
        } catch (HttpResponseException e) {
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
     * @param linkPath
     *            The fully qualified path of the new snaplink.
     * @param objectPath
     *            The fully qualified path of the object to link against.
     * @param headers
     *            Optional {@link HttpHeaders}. Consult the Manta api for more header information.
     * @throws IOException
     * @throws MantaCryptoException
     *             If there's an exception while signing the request.
     * @throws MantaClientHttpResponseException
     *             If a http status code > 300 is returned.
     */
    public void putSnapLink(String linkPath, String objectPath, HttpHeaders headers) throws MantaCryptoException,
            MantaClientHttpResponseException, IOException {
        LOG.debug(String.format("entering putLink with link %s, path %s", linkPath, objectPath));
        GenericUrl url = new GenericUrl(url_ + formatPath(linkPath));
        HttpContent content = new EmptyContent();
        HttpRequest request = HTTP_REQUEST_FACTORY.buildPutRequest(url, content);
        if (headers != null) {
            request.setHeaders(headers);
        }
        httpSigner_.signRequest(request);
        request.getHeaders().setContentType(LINK_CONTENT_TYPE);
        request.getHeaders().setLocation(formatPath(objectPath));
        HttpResponse response = null;
        try {
            response = request.execute();
            LOG.debug(String.format("got response code %s, header %s ", response.getStatusCode(), response.getHeaders()));
        } catch (HttpResponseException e) {
            throw new MantaClientHttpResponseException(e);
        } finally {
            if (response != null) {
                response.disconnect();
            }
        }
    }

    /**
     * Format the path according to RFC3986
     * 
     * @param path
     *            the raw path string.
     * @return the URI formatted string with the exception of '/' which is special in manta.
     * @throws UnsupportedEncodingException
     *             If UTF-8 is not supported on this system.
     */
    private static String formatPath(String path) throws UnsupportedEncodingException {
        // first split the path by slashes.
        String[] elements = path.split("/");
        StringBuilder encodedPath = new StringBuilder();
        for (String string : elements) {
            if (string.equals("")) {
                continue;
            }
            encodedPath.append("/").append(URLEncoder.encode(string, "UTF-8"));
        }
        return encodedPath.toString();
    }
}
