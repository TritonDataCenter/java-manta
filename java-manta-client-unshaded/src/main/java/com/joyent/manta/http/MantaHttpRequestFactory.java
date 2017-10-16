/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.AuthenticationConfigurator;
import com.joyent.manta.exception.ConfigurationException;
import com.joyent.manta.exception.MantaClientException;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpRequest;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Helper class for creating {@link org.apache.http.client.methods.HttpRequestBase} objects,
 * a.k.a. {@link org.apache.http.client.methods.HttpUriRequest}.
 */
public class MantaHttpRequestFactory {

    /**
     * Default HTTP headers to send to all requests to Manta.
     */
    private static final Header[] HEADERS = {
            new BasicHeader(MantaHttpHeaders.ACCEPT_VERSION, "~1.0"),
            new BasicHeader(HttpHeaders.ACCEPT, "application/json, */*")
    };

    private static final RequestIdInterceptor INTERCEPTOR_REQUEST_ID = new RequestIdInterceptor();

    private final AuthenticationConfigurator authConfig;

    /**
     * Interceptor for signing requests than can dynamically react to modifications in the provided configuration.
     */
    private final DynamicHttpSignatureRequestInterceptor authInterceptor;

    private final String url;

    /**
     * Build a new request factory based on an {@link AuthenticationConfigurator}.
     *
     * @param authConfig the config from which to extract base a base URL and to provide to the interceptor
     */
    public MantaHttpRequestFactory(final AuthenticationConfigurator authConfig) {
        this.authConfig = Validate.notNull(authConfig, "AuthenticationConfigurator must not be null");
        this.authInterceptor = new DynamicHttpSignatureRequestInterceptor(authConfig);
        this.url = null;
    }

    /**
     * Create an instance of the request factory based on the provided url and no authentication.
     *
     * @param url the base url
     */
    public MantaHttpRequestFactory(final String url) {
        this.authConfig = null;
        this.authInterceptor = null;
        this.url = Validate.notNull(url, "URL must not be null");;
    }

    /**
     * Convenience method used for building DELETE operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpDelete delete(final String path) {
        final HttpDelete request = new HttpDelete(uriForPath(path));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building DELETE operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpDelete delete(final String path, final List<NameValuePair> params) {
        final HttpDelete request = new HttpDelete(uriForPath(path, params));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building GET operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpGet get(final String path) {
        final HttpGet request = new HttpGet(uriForPath(path));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building GET operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpGet get(final String path, final List<NameValuePair> params) {
        final HttpGet request = new HttpGet(uriForPath(path, params));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building HEAD operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpHead head(final String path) {
        final HttpHead request = new HttpHead(uriForPath(path));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building HEAD operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpHead head(final String path, final List<NameValuePair> params) {
        final HttpHead request = new HttpHead(uriForPath(path, params));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building POST operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPost post(final String path) {
        final HttpPost request = new HttpPost(uriForPath(path));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building POST operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPost post(final String path, final List<NameValuePair> params) {
        final HttpPost request = new HttpPost(uriForPath(path, params));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building PUT operations.
     * @param path path to resource
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPut put(final String path) {
        final HttpPut request = new HttpPut(uriForPath(path));
        prepare(request);
        return request;
    }

    /**
     * Convenience method used for building PUT operations.
     * @param path path to resource
     * @param params list of query parameters to use in operation
     * @return instance of configured {@link org.apache.http.client.methods.HttpRequestBase} object.
     */
    public HttpPut put(final String path, final List<NameValuePair> params) {
        final HttpPut request = new HttpPut(uriForPath(path, params));
        prepare(request);
        return request;
    }

    private void prepare(final HttpRequest request) {
        request.setHeaders(HEADERS);

        try {
            INTERCEPTOR_REQUEST_ID.process(request, null);

            if (authInterceptor != null) {
                authInterceptor.process(request, null);
            }
        } catch (HttpException | IOException e) {
            throw new MantaClientException("Failed to prepare request", e);
        }
    }

    /**
     * Derives Manta URI for a given path.
     *
     * @param path full path
     * @return full URI as string of resource
     */
    protected String uriForPath(final String path) {
        Validate.notNull(path, "Path must not be null");

        final String format;
        if (path.startsWith("/")) {
            format = "%s%s";
        } else {
            format = "%s/%s";
        }


        final String baseURL;
        if (authConfig != null) {
            baseURL = authConfig.getURL();
        } else {
            baseURL = url;
        }

        return String.format(format, baseURL, path);
    }

    /**
     * Derives Manta URI for a given path with the passed query
     * parameters.
     *
     * @param path full path
     * @param params query parameters to add to the URI
     * @return full URI as string of resource
     */
    protected String uriForPath(final String path, final List<NameValuePair> params) {
        Validate.notNull(path, "Path must not be null");
        Validate.notNull(params, "Params must not be null");

        try {
            final URIBuilder uriBuilder = new URIBuilder(uriForPath(path));
            uriBuilder.addParameters(params);
            return uriBuilder.build().toString();
        } catch (final URISyntaxException e) {
            throw new ConfigurationException(String.format("Invalid path in URI: %s", path));
        }
    }
}
