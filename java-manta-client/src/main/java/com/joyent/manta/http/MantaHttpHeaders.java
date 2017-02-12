/*
 * Copyright (c) 2016-2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.http;

import com.joyent.manta.client.MantaMetadata;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.util.MantaUtils;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.HeaderGroup;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Object encapsulating the HTTP headers to be sent to the Manta API.
 * When non-standard HTTP headers are used as part of a PUT request to
 * Manta, they are stored as metadata about an object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaHttpHeaders implements Map<String, Object>, Serializable {
    private static final long serialVersionUID = -2547826815126982339L;

    /**
     * HTTP header for Manta durability level.
     */
    public static final String HTTP_DURABILITY_LEVEL = "durability-level";

    /**
     * HTTP header for RBAC roles.
     */
    public static final String HTTP_ROLE_TAG = "Role-Tag";

    /**
     * HTTP header passed to Manta that creates a unique request id for debugging.
     */
    public static final String REQUEST_ID = "x-request-id";

    /**
     * HTTP header containing the md5 value for the object in Manta.
     */
    public static final String COMPUTED_MD5 = "computed-md5";

    /**
     * HTTP header containing the sha256 value for this object as calculated on the client.
     */
    public static final String CONTENT_SHA256 = "m-content-sha256";

    /**
     * HTTP header containing the size of a set of results returned from Manta.
     */
    public static final String RESULT_SET_SIZE = "result-set-size";

    /**
     * HTTP header indicating the version of the API to accept - used by Restify.
     */
    public static final String ACCEPT_VERSION = "accept-version";

    /**
     * Header that indicates that a file is encrypted using client-side encryption
     * and specifies the version of client-side encryption.
     */
    public static final String ENCRYPTION_TYPE = "m-encrypt-type";

    /**
     * Header storing a unique id for the key used to encrypt.
     */
    public static final String ENCRYPTION_KEY_ID = "m-encrypt-key-id";

    /**
     * The initialization vector (IV) used when encrypting.
     */
    public static final String ENCRYPTION_IV = "m-encrypt-iv";

    /**
     * The name of the HMAC implementation used to authenticate ciphertext.
     */
    public static final String ENCRYPTION_HMAC_TYPE = "m-encrypt-hmac-type";

    /**
     * The number of bytes used to store an AEAD cipher's authentication data.
     */
    public static final String ENCRYPTION_AEAD_TAG_LENGTH = "m-encrypt-aead-tag-length";

    /**
     * The name of the cipher used to encrypt (<code>cipher/mode/padding state</code>).
     */
    public static final String ENCRYPTION_CIPHER = "m-encrypt-cipher";

    /**
     * The value of this header is the total amount of bytes of the plaintext content represented as an integer.
     */
    public static final String ENCRYPTION_PLAINTEXT_CONTENT_LENGTH = "m-encrypt-plaintext-content-length";

    /**
     * The value of this header is a base64 encoded encrypted data blob containing
     * encrypted metadata.
     */
    public static final String ENCRYPTION_METADATA = "m-encrypt-metadata";

    /**
     * The initialization vector (IV) used to encrypt the encrypted metadata.
     */
    public static final String ENCRYPTION_METADATA_IV = "m-encrypt-metadata-iv";

    /**
     * The name of the HMAC implementation used to authenticate metadata ciphertext.
     */
    public static final String ENCRYPTION_METADATA_HMAC = "m-encrypt-metadata-hmac";

    /**
     * When metadata uses AEAD, the number of bytes used to store an AEAD cipher's authentication data.
     */
    public static final String ENCRYPTION_METADATA_AEAD_TAG_LENGTH = "m-encrypt-metadata-aead-tag-length";

    /**
     * Encrypted metadata header that stores the object's real content-type.
     */
    public static final String ENCRYPTED_CONTENT_TYPE = "e-content-type";

    /**
     * The headers that are directly related to an object and not to an object's
     * encrypted metadata.
     */
    public static final String[] ENCRYPTED_ENTITY_HEADERS = new String[] {
            ENCRYPTION_CIPHER,
            ENCRYPTION_TYPE,
            ENCRYPTION_AEAD_TAG_LENGTH,
            ENCRYPTION_HMAC_TYPE,
            ENCRYPTION_IV,
            ENCRYPTION_KEY_ID,
            ENCRYPTION_PLAINTEXT_CONTENT_LENGTH
    };

    /**
     * HttpHeaders delegate which is wrapped by this class.
     */
    private final transient CaseInsensitiveMap<String, Object> wrappedHeaders =
            new CaseInsensitiveMap<>();

    /**
     * Creates an empty instance.
     */
    public MantaHttpHeaders() {
    }

    /**
     * Creates an instance with headers prepopulated from the specified {@link Map}.
     *
     * @param headers headers to prepopulate
     */
    public MantaHttpHeaders(final Map<? extends String, ?> headers) {
        Validate.notNull(headers, "Headers must not be null");
        wrappedHeaders.putAll(headers);
    }

    /**
     * Creates an instance with headers prepopulated from an existing
     * {@link MantaObject} instance.
     *
     * @param mantaObject Manta object to read headers from
     */
    public MantaHttpHeaders(final MantaObject mantaObject) {
        wrappedHeaders.putAll(mantaObject.getHttpHeaders().wrappedHeaders);
    }

    /**
     * Creates an instance with headers prepopulated from Apache HTTP client.
     *
     * @param headers headers to prepopulate
     */
    public MantaHttpHeaders(final Header[] headers) {
        for (Header header : headers) {
            if (header == null) {
                continue;
            }

            if (StringUtils.isEmpty(header.getValue())) {
                put(header.getName(), null);
                continue;
            }

            final String name = header.getName();

            Object currentValue = get(name);

            if (currentValue == null) {
                put(name, header.getValue());
            } else if (currentValue instanceof Collection) {
                @SuppressWarnings("unchecked")
                Collection<Object> values = ((Collection<Object>)currentValue);
                values.add(header.getValue());
                put(name, values);
            } else {
                List<Object> values = new ArrayList<>(2);
                values.add(currentValue);
                values.add(header.getValue());
                put(name, values);
            }
        }
    }

    /**
     * Converts a key value to a collection of {@link Header} objects.
     * @param name header name
     * @param value header value
     * @return as list of headers
     */
    private HeaderGroup parseHeaderKeyValue(final String name, final Object value) {
        HeaderGroup group = new HeaderGroup();

        if (value == null) {
            group.addHeader(new BasicHeader(name, null));
            return group;
        }

        final Class<?> valueClass = value.getClass();

        if (value instanceof Iterable<?>) {
            Iterable<?> iterable = (Iterable<?>)value;

            for (Object multiple : iterable) {
                final Header header = new BasicHeader(name, MantaUtils.asString(multiple));
                group.addHeader(header);
            }
        } else if (valueClass.isArray()) {
            Object[] array = (Object[])value;

            for (Object multiple : array) {
                final Header header = new BasicHeader(name, MantaUtils.asString(multiple));
                group.addHeader(header);
            }
        } else {
            group.addHeader(new BasicHeader(name, MantaUtils.asString(value)));
        }

        return group;
    }

    /**
     * Returns the headers as an array of {@link org.apache.http.Header} instances.
     *
     * @return an array of {@link org.apache.http.Header} instances
     */
    public Header[] asApacheHttpHeaders() {
        if (wrappedHeaders.isEmpty()) {
            return new Header[0];
        }

        final int length = wrappedHeaders.size();
        final Header[] headers = new Header[length];
        final MapIterator<String, Object> itr = wrappedHeaders.mapIterator();

        int i = 0;
        while (itr.hasNext()) {
            String key = itr.next();
            Object val = itr.getValue();

            final HeaderGroup extracted = parseHeaderKeyValue(key, val);
            headers[i++] = extracted.getCondensedHeader(key);
        }

        if (length == i) {
            return headers;
        }

        return Arrays.copyOfRange(headers, 0, i);
    }

    /**
     * Returns all headers corresponding to manta-service custom metadata.
     * The metadata values will be typed according to the underlying header implementation.
     *
     * @return custom metadata as a {@link java.util.Map}
     */
    public Map<String, ?> metadata() {
        final Map<String, Object> metadata = new HashMap<>();
        for (Map.Entry<String, Object> entry : wrappedHeaders.entrySet()) {
            if (entry.getKey().startsWith("m-")) {
                metadata.put(entry.getKey(), entry.getValue());
            }
        }

        return metadata;
    }

    /**
     * Returns all headers corresponding to manta-service custom metadata.
     * The metadata values will be serialized as {@link java.lang.String}
     *
     * @return custom metadata as a {@link java.util.Map}
     */
    public Map<String, String> metadataAsStrings() {
        final Map<String, String> metadata = new HashMap<>();
        for (Map.Entry<String, Object> entry : wrappedHeaders.entrySet()) {
            boolean hasMPrefix = entry.getKey().startsWith(MantaMetadata.METADATA_PREFIX);
            boolean hasEPrefix = entry.getKey().startsWith(MantaMetadata.ENCRYPTED_METADATA_PREFIX);

            if (hasMPrefix || hasEPrefix) {
                metadata.put(entry.getKey(), MantaUtils.asString(entry.getValue()));
            }
        }

        return metadata;
    }

    /**
     * Returns the value of request id header.
     *
     * @return the request id header value
     */
    public String getRequestId() {
        return getMultipleValuesAsString(REQUEST_ID);
    }

    /**
     * Sets the number of replicated copies of the object in Manta.
     *
     * @param copies number of copies
     */
    public void setDurabilityLevel(final int copies) {
        if (copies < 0) {
            String msg = String.format("Copies must be 1 or greater for user objects. "
                    + "For jobs and system objects it can be 0. Actual value: %d", copies);
            throw new IllegalArgumentException(msg);
        }

        put(HTTP_DURABILITY_LEVEL, String.valueOf(copies));
    }

    /**
     * Gets the number of replicated copies of the object in Manta.
     *
     * @return copies number of copies
     */
    public Integer getDurabilityLevel() {
        return getIntegerFromHeader(HTTP_DURABILITY_LEVEL);
    }

    /**
     * Sets the header defining RBAC roles used for this object.
     *
     * @param roles roles associated with object
     */
    public void setRoles(final Set<String> roles) {
        Validate.notNull(roles, "Roles must not be null");

        /* Set roles as a single HTTP header with each role delimited by
         * a comma.
         */
        put(HTTP_ROLE_TAG, MantaUtils.asString(roles));
    }

    /**
     * Gets the header defining RBAC roles used for this object.
     *
     * @return roles associated with object
     */
    public Set<String> getRoles() {
        final Object value = get(HTTP_ROLE_TAG);

        if (value == null) {
            return Collections.emptySet();
        }

        final HashSet<String> roles = new HashSet<>();

        if (value instanceof Iterable<?>) {
            ((Iterable<?>)value).forEach(o -> {
                if (o != null) {
                    roles.add(o.toString());
                }
            });
        } else if (value.getClass().isArray()) {
            for (Object o : (Object[])value) {
                if (o != null) {
                    roles.add(o.toString());
                }
            }
        } else {
            String line = value.toString();
            roles.addAll(MantaUtils.fromCsv(line));
        }

        /* The result may come to us as a CSV. In that case we treat each
         * value separated by a comma as a single role.
         */
        if (roles.size() == 1) {
            String line = roles.iterator().next();
            roles.clear();
            roles.addAll(MantaUtils.fromCsv(line));
        }

        return Collections.unmodifiableSet(roles);
    }

    /**
     * Parses the value of the Result-Set-Size HTTP header returned from Manta.
     *
     * @return long value of header value, or null if it can't be found or parsed
     */
    public Long getResultSetSize() {
        return getLongFromHeader(RESULT_SET_SIZE);
    }

    /**
     * Returns the first {@code "Accept"} header or {@code null} for none.
     *
     * @return {@code "Accept"} header value as a {@code java.lang.String} value
     */
    public String getAccept() {
        return getMultipleValuesAsString(HttpHeaders.ACCEPT);
    }

    /**
     * Sets the {@code "Accept"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param accept {@code java.lang.String} value for {@code "Accept"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAccept(final String accept) {
        put(HttpHeaders.ACCEPT, accept);
        return this;
    }

    /**
     * Returns the first {@code "Accept-Encoding"} header or {@code null} for none.
     *
     * @return {@code "Accept-Encoding"} header value as a {@code java.lang.String} value
     */
    public String getAcceptEncoding() {
        return Objects.toString(get(HttpHeaders.ACCEPT_ENCODING));
    }

    /**
     * Sets the {@code "Accept-Encoding"} header or {@code null} for none.
     *
     * <p>
     * By default, this is {@code "gzip"}.
     * </p>
     *
     * @param acceptEncoding {@code java.lang.String} value for {@code "Accept-Encoding"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAcceptEncoding(final String acceptEncoding) {
        put(HttpHeaders.ACCEPT_ENCODING, acceptEncoding);
        return this;
    }

    /**
     * Returns the first {@code "Authorization"} header or {@code null} for none.
     *
     * @return {@code "Authorization"} header value as a {@code java.lang.String} value
     */
    public String getAuthorization() {
        return getMultipleValuesAsString(HttpHeaders.AUTHORIZATION);
    }

    /**
     * Returns all {@code "Authorization"} headers or {@code null} for none.
     *
     * @return {@code "Authorization"} headers as a {@code java.util.List} of {@code java.lang.String} values.
     */
    public List<String> getAuthorizationAsList() {
        return getHeaderStringValues(HttpHeaders.AUTHORIZATION);
    }

    /**
     * Sets the {@code "Authorization"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param authorization {@code java.lang.String} value for {@code "Accept-Encoding"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAuthorization(final String authorization) {
        put(HttpHeaders.AUTHORIZATION, authorization);
        return this;
    }

    /**
     * Sets the {@code "Authorization"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param authorization a {@code java.util.List} of {@code java.lang.String} values
     *                      for {@code "Authorization"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAuthorization(final List<String> authorization) {
        put(HttpHeaders.AUTHORIZATION, authorization);
        return this;
    }

    /**
     * Returns the first {@code "Cache-Control"} header or {@code null} for none.
     *
     * @return {@code "Cache-Control"} header value as a {@code java.lang.String} value
     */
    public String getCacheControl() {
        return getMultipleValuesAsString(HttpHeaders.CACHE_CONTROL);
    }

    /**
     * Sets the {@code "Cache-Control"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param cacheControl {@code java.lang.String} value for {@code "Cache-Control"} header.
     * @return this instance
     */
    public MantaHttpHeaders setCacheControl(final String cacheControl) {
        put(HttpHeaders.CACHE_CONTROL, cacheControl);
        return this;
    }

    /**
     * Returns the first {@code "Content-Encoding"} header or {@code null} for none.
     *
     * @return {@code "Content-Encoding"} header value as a {@code java.lang.String} value
     */
    public String getContentEncoding() {
        return getMultipleValuesAsString(HttpHeaders.CONTENT_ENCODING);
    }

    /**
     * Sets the {@code "Content-Encoding"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param contentEncoding {@code java.lang.String} value for {@code "Content-Encoding"} header
     * @return this instance
     */
    @SuppressWarnings("UnusedReturnValue")
    public MantaHttpHeaders setContentEncoding(final String contentEncoding) {
        put(HttpHeaders.CONTENT_ENCODING, contentEncoding);
        return this;
    }

    /**
     * Returns the first {@code "Content-Length"} header or {@code null} for none.
     *
     * @return {@code "Content-Length"} header value as a {@code java.lang.Long} value
     */
    public Long getContentLength() {
        return getLongFromHeader(HttpHeaders.CONTENT_LENGTH);
    }

    /**
     * Sets the {@code "Content-Length"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param contentLength {@code java.lang.Long} value for the {@code "Content-Length"} header
     * @return this instance
     */
    @SuppressWarnings("UnusedReturnValue")
    public MantaHttpHeaders setContentLength(final Long contentLength) {
        wrappedHeaders.put(HttpHeaders.CONTENT_LENGTH, contentLength);
        return this;
    }

    /**
     * Returns the first {@code "Content-MD5"} header or {@code null} for none.
     *
     * @return {@code "Content-MD5"} header value as a {@code java.lang.String} value
     */
    public String getContentMD5() {
        return getMultipleValuesAsString((HttpHeaders.CONTENT_MD5));
    }

    /**
     * Sets the {@code "Content-MD5"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param contentMD5 {@code java.lang.String} value for {@code "Content-MD5"} header.
     * @return this instance
     */
    public MantaHttpHeaders setContentMD5(final String contentMD5) {
        put(HttpHeaders.CONTENT_MD5, contentMD5);
        return this;
    }

    /**
     * Returns the first {@code "Content-Range"} header or {@code null} for none.
     *
     * @return {@code "Content-Range"} header value as a {@code java.lang.String} value
     */
    public String getContentRange() {
        return getMultipleValuesAsString(HttpHeaders.CONTENT_RANGE);
    }

    /**
     * Sets the {@code "Content-Range"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param contentRange {@code java.lang.String} value for {@code "Content-Range"} header.
     * @return this instance
     */
    public MantaHttpHeaders setContentRange(final String contentRange) {
        put(HttpHeaders.CONTENT_RANGE, contentRange);
        return this;
    }

    /**
     * Returns the first {@code "Content-Type"} header or {@code null} for none.
     *
     * @return {@code "Content-Type"} header value as a {@code java.lang.String} value
     */
    public String getContentType() {
        return getMultipleValuesAsString(HttpHeaders.CONTENT_TYPE);
    }

    /**
     * Sets the {@code "Content-Type"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param contentType {@code java.lang.String} value for {@code "Content-Type"} header.
     * @return this instance
     */
    public MantaHttpHeaders setContentType(final String contentType) {
        put(HttpHeaders.CONTENT_TYPE, contentType);
        return this;
    }

    /**
     * Returns the first {@code "Cookie"} header or {@code null} for none.
     *
     * <p>
     * See <a href='http://tools.ietf.org/html/rfc6265'>Cookie Specification.</a>
     * </p>
     *
     * @return {@code "Cookie"} header value as a {@code java.lang.String} value
     */
    @Deprecated
    public String getCookie() {
        return getMultipleValuesAsString("Cookie");
    }

    /**
     * Sets the {@code "Cookie"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param cookie {@code java.lang.String} value for {@code "Cookie"} header.
     * @return this instance
     */
    @Deprecated
    public MantaHttpHeaders setCookie(final String cookie) {
        put("Cookie", cookie);
        return this;
    }

    /**
     * Returns the first {@code "Date"} header or {@code null} for none.
     *
     * @return {@code "Date"} header value as a {@code java.lang.String} value
     */
    public String getDate() {
        return getMultipleValuesAsString(HttpHeaders.DATE);
    }

    /**
     * Sets the {@code "Date"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param date {@code java.lang.String} value for {@code "Date"} header.
     * @return this instance
     */
    public MantaHttpHeaders setDate(final String date) {
        put(HttpHeaders.DATE, date);
        return this;
    }

    /**
     * Returns the first {@code "ETag"} header or {@code null} for none.
     *
     * @return {@code "ETag"} header value as a {@code java.lang.String} value
     */
    public String getETag() {
        return getMultipleValuesAsString(HttpHeaders.ETAG);
    }

    /**
     * Sets the {@code "ETag"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param etag {@code java.lang.String} value for {@code "ETag"} header.
     * @return this instance
     */
    @SuppressWarnings("UnusedReturnValue")
    public MantaHttpHeaders setETag(final String etag) {
        put(HttpHeaders.ETAG, etag);
        return this;
    }

    /**
     * Returns the first {@code "Expires"} header or {@code null} for none.
     *
     * @return {@code "Expires"} header value as a {@code java.lang.String} value
     */
    public String getExpires() {
        return getMultipleValuesAsString(HttpHeaders.EXPIRES);
    }

    /**
     * Sets the {@code "Expires"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param expires {@code java.lang.String} value for {@code "Expires"} header.
     * @return this instance
     */
    public MantaHttpHeaders setExpires(final String expires) {
        put(HttpHeaders.EXPIRES, expires);
        return this;
    }

    /**
     * Returns the first {@code "If-Modified-Since"} header or {@code null} for none.
     *
     * @return {@code "If-Modified-Since"} header value as a {@code java.lang.String} value
     */
    public String getIfModifiedSince() {
        return getMultipleValuesAsString(HttpHeaders.IF_MODIFIED_SINCE);
    }

    /**
     * Sets the {@code "If-Modified-Since"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param ifModifiedSince {@code java.lang.String} value for {@code "If-Modified-Since"} header.
     * @return this instance
     */
    public MantaHttpHeaders setIfModifiedSince(final String ifModifiedSince) {
        put(HttpHeaders.IF_MODIFIED_SINCE, ifModifiedSince);
        return this;
    }

    /**
     * Returns the first {@code "If-Match"} header or {@code null} for none.
     *
     * @return {@code "If-Match"} header value as a {@code java.lang.String} value
     */
    public String getIfMatch() {
        return getMultipleValuesAsString(HttpHeaders.IF_MATCH);
    }

    /**
     * Sets the {@code "If-Match"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param ifMatch {@code java.lang.String} value for {@code "If-Match"} header.
     * @return this instance
     */
    public MantaHttpHeaders setIfMatch(final String ifMatch) {
        put(HttpHeaders.IF_MATCH, ifMatch);
        return this;
    }

    /**
     * Returns the first {@code "If-None-Match"} header or {@code null} for none.
     *
     * @return {@code "If-None-Match"} header value as a {@code java.lang.String} value
     */
    public String getIfNoneMatch() {
        return getMultipleValuesAsString(HttpHeaders.IF_NONE_MATCH);
    }

    /**
     * Sets the {@code "If-None-Match"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param ifNoneMatch {@code java.lang.String} value for {@code "If-None-Match"} header.
     * @return this instance
     */
    public MantaHttpHeaders setIfNoneMatch(final String ifNoneMatch) {
        put(HttpHeaders.IF_NONE_MATCH, ifNoneMatch);
        return this;
    }

    /**
     * Returns the first {@code "If-Unmodified-Since"} header or {@code null} for none.
     *
     * @return {@code "If-Unmodified-Since"} header value as a {@code java.lang.String} value
     */
    public String getIfUnmodifiedSince() {
        return getMultipleValuesAsString(HttpHeaders.IF_MODIFIED_SINCE);
    }

    /**
     * Sets the {@code "If-Unmodified-Since"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param ifUnmodifiedSince {@code java.lang.String} value for {@code "If-Unmodified-Since"} header.
     * @return this instance
     */
    public MantaHttpHeaders setIfUnmodifiedSince(final String ifUnmodifiedSince) {
        put(HttpHeaders.IF_UNMODIFIED_SINCE, ifUnmodifiedSince);
        return this;
    }

    /**
     * Returns the first {@code "If-Range"} header or {@code null} for none.
     *
     * @return {@code "If-Range"} header value as a {@code java.lang.String} value
     */
    public String getIfRange() {
        return getMultipleValuesAsString(HttpHeaders.IF_RANGE);
    }

    /**
     * Sets the {@code "If-Range"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param ifRange {@code java.lang.String} value for {@code "If-Range"} header.
     * @return this instance
     */
    public MantaHttpHeaders setIfRange(final String ifRange) {
        put(HttpHeaders.IF_RANGE, ifRange);
        return this;
    }

    /**
     * Returns the first {@code "Last-Modified"} header or {@code null} for none.
     *
     * @return {@code "Last-Modified"} header value as a {@code java.lang.String} value
     */
    public String getLastModified() {
        return getMultipleValuesAsString(HttpHeaders.LAST_MODIFIED);
    }

    /**
     * Sets the {@code "Last-Modified"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param lastModified {@code java.lang.String} value for {@code "Last-Modified"} header.
     * @return this instance
     */
    @SuppressWarnings("UnusedReturnValue")
    public MantaHttpHeaders setLastModified(final String lastModified) {
        put(HttpHeaders.LAST_MODIFIED, lastModified);
        return this;
    }

    /**
     * Returns the first {@code "Location"} header or {@code null} for none.
     *
     * @return {@code "Location"} header value as a {@code java.lang.String} value
     */
    public String getLocation() {
        return getMultipleValuesAsString(HttpHeaders.LOCATION);
    }

    /**
     * Sets the {@code "Location"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param location {@code java.lang.String} value for {@code "Location"} header.
     * @return this instance
     */
    public MantaHttpHeaders setLocation(final String location) {
        put(HttpHeaders.LOCATION, location);
        return this;
    }

    /**
     * Returns the first {@code "MIME-Version"} header or {@code null} for none.
     *
     * @return {@code "MIME-Version"} header value as a {@code java.lang.String} value
     */
    @Deprecated
    public String getMimeVersion() {
        return getMultipleValuesAsString("MIME-Version");
    }

    /**
     * Sets the {@code "MIME-Version"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param mimeVersion {@code java.lang.String} value for {@code "MIME-Version"} header.
     * @return this instance
     */
    @Deprecated
    public MantaHttpHeaders setMimeVersion(final String mimeVersion) {
        put("MIME-Version", mimeVersion);
        return this;
    }

    /**
     * Returns the first {@code "Range"} header or {@code null} for none.
     *
     * @return {@code "Range"} header value as a {@code java.lang.String} value
     */
    public String getRange() {
        return getMultipleValuesAsString(HttpHeaders.RANGE);
    }

    /**
     * Sets the {@code "Range"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param range {@code java.lang.String} value for {@code "Range"} header.
     * @return this instance
     */
    public MantaHttpHeaders setRange(final String range) {
        put(HttpHeaders.RANGE, range);
        return this;
    }

    /**
     * A convenience method to set the {@code "Range"} header by
     * specifying start and end byte positions instead of requiring
     * the caller to pre-assemble a string.  This method only handles
     * a single multi-byte range.  For example, this method can
     * express the byte range "5-10" but not "0,5-10,100-200".

     * @param start The byte position to start at. If {@code null},
     * then the range is computed from the <strong>end</strong>.
     * @param end The ending byte position. If {@code null} then the
     * range has no end bound.
     * @return this instance
     */
    public MantaHttpHeaders setByteRange(final Long start, final Long end) {
        String prefix = "bytes=";
        String expression;
        if (start == null && end == null) {
            throw new IllegalArgumentException("one of range {start,end} must be non-null");
        } else if (end == null) {
            if (start < 0) {
                throw new IllegalArgumentException("range start must be non-negative");
            }
            expression = String.format("%d-", start);

        } else if (start == null) {
            if (end <= 0) {
                throw new IllegalArgumentException("range end from last byte must be positive");
            }
            expression = String.format("-%d", end);
        } else {
            if (start > end) {
                throw new IllegalArgumentException("range start must be less than end");
            } else if (start < 0 || end < 0) {
                throw new IllegalArgumentException("range {start,end} must be non-negative");
            }
            expression = String.format("%d-%d", start, end);
        }
        String rangeStr = prefix + expression;
        put(HttpHeaders.RANGE, rangeStr);
        return this;
    }

    /**
     * Translates the range request header into two values. The first value
     * is the starting bytes of the binary file to read and the second value
     * is the ending bytes of the file to read. If the range indicates the
     * end of a file (unlimited), then the end value will be set to null.
     * Likewise, if the start position is unknown, it will be set to null.
     *
     * @return two value array containing the start and the end of a byte range as Long
     */
    public Long[] getByteRange() {
        final String rangeString = getRange();
        Validate.notNull(rangeString, "Range HTTP must not be null");
        String[] rangeValuesStrings = StringUtils.split(rangeString, "bytes=");
        Validate.isTrue(rangeValuesStrings.length == 1,
                "Range header value doesn't begin with string: bytes=");

        final String byteRange = rangeValuesStrings[0];

        Validate.isTrue(StringUtils.split(byteRange, ",").length == 1,
                "Multi-range requests are not supported");

        String[] rangeParts = StringUtils.split(byteRange, "-");
        Validate.isTrue(StringUtils.countMatches(byteRange, "-") < 2,
                "Cannot end or start with a negative number");

        Long startPos = null;
        Long endPos = null;

        if (StringUtils.startsWith(byteRange, "-")) {
            endPos = Long.parseLong(byteRange);
        } else if (StringUtils.endsWith(byteRange, "-")) {
            startPos = Long.parseLong(byteRange.split("-")[0]);
        } else if (rangeParts.length == 2) {
            startPos = Long.parseUnsignedLong(rangeParts[0]);
            endPos = Long.parseUnsignedLong(rangeParts[1]);
        } else {
            throw new IllegalArgumentException("range must exist with - separator");
        }

        return new Long[] {startPos, endPos};
    }

    /**
     * Returns the first {@code "Retry-After"} header or {@code null} for none.
     *
     * @return {@code "Retry-After"} header value as a {@code java.lang.String} value
     */
    public String getRetryAfter() {
        return getMultipleValuesAsString(HttpHeaders.RETRY_AFTER);
    }

    /**
     * Sets the {@code "Retry-After"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param retryAfter {@code java.lang.String} value for {@code "Retry-After"} header.
     * @return this instance
     */
    public MantaHttpHeaders setRetryAfter(final String retryAfter) {
        put(HttpHeaders.RETRY_AFTER, retryAfter);
        return this;
    }

    /**
     * Returns the first {@code "User-Agent"} header or {@code null} for none.
     *
     * @return {@code "User-Agent"} header value as a {@code java.lang.String} value
     */
    public String getUserAgent() {
        return getMultipleValuesAsString(HttpHeaders.USER_AGENT);
    }

    /**
     * Sets the {@code "User-Agent"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param userAgent {@code java.lang.String} value for {@code "User-Agent"} header.
     * @return this instance
     */
    public MantaHttpHeaders setUserAgent(final String userAgent) {
        put(HttpHeaders.USER_AGENT, userAgent);
        return this;
    }

    /**
     * Returns the first {@code "WWW-Authenticate"} header or {@code null} for none.
     *
     * @return {@code "WWW-Authenticate"} header value as a {@code java.lang.String} value
     */
    public String getAuthenticate() {
        return getMultipleValuesAsString(HttpHeaders.WWW_AUTHENTICATE);
    }

    /**
     * Returns all {@code "WWW-Authenticate"} headers or {@code null} for none.
     *
     * @return {@code "WWW-Authenticate"} headers as a {@code java.util.List} of {@code java.lang.String} values
     */
    public List<String> getAuthenticateAsList() {
        return getHeaderStringValues(HttpHeaders.WWW_AUTHENTICATE);
    }

    /**
     * Sets the {@code "WWW-Authenticate"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param authenticate {@code java.lang.String} value for {@code "WWW-Authenticate"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAuthenticate(final String authenticate) {
        put(HttpHeaders.WWW_AUTHENTICATE, authenticate);
        return this;
    }

    /**
     * Returns the first {@code "Age"} header or {@code null} for none.
     *
     * @return {@code "Age"} header value as a {@code java.lang.Long} value
     */
    public Long getAge() {
        return getLongFromHeader(HttpHeaders.AGE);
    }

    /**
     * Sets the {@code "Age"} header or {@code null} for none.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param age {@code java.lang.Long} value for {@code "Age"} header.
     * @return this instance
     */
    public MantaHttpHeaders setAge(final Long age) {
        put(HttpHeaders.AGE, age);
        return this;
    }

    /**
     * Sets the {@code authorization} header as specified in <a
     * href="http://tools.ietf.org/html/rfc2617#section-2">Basic Authentication Scheme</a>.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param username {@code java.lang.String} value for the username component of the authorization header
     * @param password {@code java.lang.String} value for the password component of the authorization header
     * @return this instance
     */
    @Deprecated
    public MantaHttpHeaders setBasicAuthentication(final String username, final String password) {
        Validate.notNull(username, "Username must not be null");
        Validate.notNull(password, "Password must not be null");

        String userPass = String.format("%s:%s", username, password);
        String encoded = Base64.getEncoder().encodeToString(userPass.getBytes(
                StandardCharsets.UTF_8));

        return setAuthorization("Basic " + encoded);
    }

    /**
     * Parses the value of the supplied header name as a Long or returns the
     * long value if it is a {@link Number}. Note: this method isn't compatible
     * with headers that return multiple values.
     *
     * @param name header name (may be any case)
     * @return long value of header
     */
    private Long getLongFromHeader(final String name) {
        String value = getFirstHeaderStringValue(name);

        if (value == null) {
            return null;
        }

        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            String msg = String.format("Error parsing header "
                    + "as long. Actual value: %s", value);
            LoggerFactory.getLogger(getClass()).warn(msg, e);
            return null;
        }
    }

    /**
     * Parses the value of the supplied header name as an Integer or returns the
     * int value if it is a {@link Number}. Note: this method isn't compatible
     * with headers that return multiple values.
     *
     * @param name header name (may be any case)
     * @return long value of header
     */
    private Integer getIntegerFromHeader(final String name) {
        String value = getFirstHeaderStringValue(name);

        if (value == null) {
            return null;
        }

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            String msg = String.format("Error parsing header "
                    + "as int. Actual value: %s", value);
            LoggerFactory.getLogger(getClass()).warn(msg, e);
            return null;
        }
    }

    /**
     * Returns the first header string value for the given header name.
     *
     * @param name header name (may be any case)
     * @return first header string value or {@code null} if not found
     */
    public String getFirstHeaderStringValue(final String name) {
        Object value = get(name);

        if (value == null) {
            return null;
        }

        HeaderGroup group = parseHeaderKeyValue(name, value);
        return group.getFirstHeader(name).getValue();
    }

    /**
     * Returns a list of the header string values for the given header name.
     *
     * @param name header name (may be any case)
     * @return header string values or empty if not found
     */
    public List<String> getHeaderStringValues(final String name) {
        Object value = get(name);

        if (value == null) {
            return null;
        }

        List<String> values = new ArrayList<>();
        HeaderGroup group = parseHeaderKeyValue(name, value);
        Header[] headers = group.getAllHeaders();

        for (Header header : headers) {
            String headerValue = header.getValue();

            if (headerValue != null) {
                values.add(headerValue);
            }
        }

        return values;
    }

    /**
     * Returns the condensed value for an HTTP header that may contain multiple
     * elements.
     *
     * @param name header name (may be any case)
     * @return Condensed HTTP header value as per RFC 2616
     */
    private String getMultipleValuesAsString(final String name) {
        Object value = get(name);

        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String)value;
        }

        HeaderGroup group = parseHeaderKeyValue(name, value);
        Header condensed = group.getCondensedHeader(name);
        final String condensedValue;

        if (condensed != null) {
            condensedValue = condensed.getValue();
        } else {
            condensedValue = null;
        }

        return condensedValue;
    }

    @Override
    public Object get(final Object name) {
        return wrappedHeaders.get(name);
    }

    /**
     * @param name name of key to retrieve string value
     * @return the value represented as a string using {@link MantaUtils#asString(Object)}
     */
    public String getAsString(final Object name) {
        return MantaUtils.asString(get(name));
    }

    @Override
    public Object put(final String fieldName, final Object value) {
        return wrappedHeaders.put(fieldName, value);
    }

    @Override
    public void putAll(final Map<? extends String, ?> map) {
        wrappedHeaders.putAll(map);
    }

    /**
     * Adds all entries from the metadata collection that are unencrypted metadata
     * entries.
     *
     * @param metadata metadata object to add metadata from
     */
    public void putAll(final MantaMetadata metadata) {
        Validate.notNull(metadata, "Metadata object must not be null");

        Set<Map.Entry<String, String>> entrySet = metadata.entrySet();

        for (Entry<String, String> next : entrySet) {
            if (next.getKey().startsWith(MantaMetadata.METADATA_PREFIX)) {
                wrappedHeaders.put(next.getKey(), next.getValue());
            }
        }
    }

    @Override
    public Object remove(final Object name) {
        return wrappedHeaders.remove(name);
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return wrappedHeaders.entrySet();
    }

    /**
     * This returns a copy of the backing map. In versions, 2.x this returned
     * the headers that didn't explicitly have setters and getters.
     *
     * @return {@code java.util.Map} of unknown key-value mappings.
     */
    @Deprecated
    public Map<String, Object> getUnknownKeys() {
        return wrappedHeaders.clone();
    }

    /**
     * Sets the map of unknown data key name to value.
     *
     * @param unknownFields {@code java.util.Map} of unknown key-value mappings
     */
    @Deprecated
    public void setUnknownKeys(final Map<String, Object> unknownFields) {
        wrappedHeaders.putAll(unknownFields);
    }

    @Override
    public int size() {
        return wrappedHeaders.size();
    }

    @Override
    public boolean isEmpty() {
        return wrappedHeaders.isEmpty();
    }

    @Override
    public boolean containsValue(final Object value) {
        return wrappedHeaders.containsValue(value);
    }

    @Override
    public boolean containsKey(final Object key) {
        return wrappedHeaders.containsKey(key);
    }

    @Override
    public void clear() {
        wrappedHeaders.clear();
    }

    @Override
    public Set<String> keySet() {
        return wrappedHeaders.keySet();
    }

    @Override
    public Collection<Object> values() {
        return wrappedHeaders.values();
    }

    /**
     * Equivalent to the put operation.
     *
     * @param fieldName field name of the header
     * @param value value for the header
     * @return this instance
     */
    @Deprecated
    public MantaHttpHeaders set(final String fieldName, final Object value) {
        put(fieldName, value);
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MantaHttpHeaders headers = (MantaHttpHeaders) o;
        return Objects.equals(
                wrappedHeaders,
                headers.wrappedHeaders
        );
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrappedHeaders);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MantaHttpHeaders{");
        sb.append("wrappedHeaders=").append(wrappedHeaders);
        sb.append('}');
        return sb.toString();
    }
}
