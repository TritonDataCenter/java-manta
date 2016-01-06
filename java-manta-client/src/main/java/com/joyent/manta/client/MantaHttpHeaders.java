package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.FieldInfo;
import com.google.api.client.util.Types;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.message.BasicHeader;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.joyent.http.signature.Signer.X_REQUEST_ID_HEADER;

/**
 * Object encapsulating the HTTP headers to be sent to the Manta API.
 * When non-standard HTTP headers are used as part of a PUT request to
 * Manta, they are stored as metadata about an object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaHttpHeaders implements Serializable {
    private static final long serialVersionUID = -2591173969776316384L;

    /**
     * HTTP header for Manta durability level.
     */
    public static final String HTTP_DURABILITY_LEVEL = "Durability-Level";

    /**
     * HTTP header for RBAC roles.
     */
    public static final String HTTP_ROLE_TAG = "Role-Tag";

    /**
     * HttpHeaders delegate which is wrapped by this class.
     */
    private final transient HttpHeaders wrappedHeaders = new HttpHeaders();

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
        Objects.requireNonNull(headers, "Headers should be present");
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
     * Creates an instance with headers prepopulated from the Google HTTP Client
     * headers class.
     *
     * @param headers headers to prepopulate
     */
    MantaHttpHeaders(final HttpHeaders headers) {
        if (headers != null) {
            wrappedHeaders.fromHttpHeaders(headers);
        }
    }


    /**
     * Creates an instance with headers prepopulated from Apache HTTP client.
     *
     * @param headers headers to prepopulate
     */
    MantaHttpHeaders(final Header[] headers) {
        for (Header header : headers) {
            if (header == null) {
                continue;
            }

            if (header.getValue() == null) {
                wrappedHeaders.put(header.getName(), null);
            }

            switch (header.getName().toLowerCase()) {
                case "content-length":
                    wrappedHeaders.setContentLength(Long.parseLong(header.getValue()));
                    break;
                case "age":
                    wrappedHeaders.setAge(Long.parseLong(header.getValue()));
                    break;
                default:
                    List<String> values = new ArrayList<>();
                    for (HeaderElement e : header.getElements()) {
                        values.add(e.getValue());
                    }
                    wrappedHeaders.set(header.getName(), values);
            }
        }
    }


    /**
     * Returns the headers as an array of {@link org.apache.http.Header} instances.
     *
     * @return an array of {@link org.apache.http.Header} instances
     */
    Header[] asApacheHttpHeaders() {
        final ArrayList<Header> headers = new ArrayList<>();

        for (Map.Entry<String, ?> entry : wrappedHeaders.entrySet()) {
            final String name = entry.getKey();
            final Object value = entry.getValue();

            final String displayName;
            FieldInfo fieldInfo = wrappedHeaders.getClassInfo().getFieldInfo(name);
            if (fieldInfo != null) {
                displayName = fieldInfo.getName();
            } else {
                displayName = name;
            }

            if (value == null) {
                headers.add(new BasicHeader(displayName, null));
                continue;
            }

            final Class<?> valueClass = value.getClass();

            if (value instanceof Iterable<?> || valueClass.isArray()) {
                for (Object multiple : Types.iterableOf(value)) {
                    final Header header = new BasicHeader(displayName, MantaUtils.asString(multiple));
                    headers.add(header);
                }
            } else {
                final Header header = new BasicHeader(displayName, MantaUtils.asString(value));
                headers.add(header);
            }
        }

        Header[] array = new Header[headers.size()];
        headers.toArray(array);

        return array;
    }


    /**
     * Returns the headers as an instance of {@link com.google.api.client.http.HttpHeaders}.
     * @return an instance of {@link com.google.api.client.http.HttpHeaders}
     */
    HttpHeaders asGoogleClientHttpHeaders() {
        return wrappedHeaders;
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
            if (entry.getKey().startsWith("m-")) {
                metadata.put(entry.getKey(), MantaUtils.asString(entry.getValue()));
            }
        }

        return metadata;
    }


    /**
     * Adds all entries of specified metadata to this metadata.
     *
     * @param metadata the metadata to be added
     */
    public void putAllMetadata(final MantaMetadata metadata) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }


    /**
     * Returns the value of request id header.
     *
     * @return the request id header value
     */
    public String getRequestId() {
        Object requestId = wrappedHeaders.get(X_REQUEST_ID_HEADER);
        if (requestId == null) {
            return null;
        }

        @SuppressWarnings("unchecked")
        Iterable<String> elements = (Iterable)requestId;
        Iterator<String> itr = elements.iterator();

        if (!itr.hasNext()) {
            return null;
        }

        return itr.next();
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

        set(HTTP_DURABILITY_LEVEL, String.valueOf(copies));
    }


    /**
     * Gets the number of replicated copies of the object in Manta.
     *
     * @return copies number of copies
     */
    public Integer getDurabilityLevel() {
        final String value = getFirstHeaderStringValue(HTTP_DURABILITY_LEVEL);

        if (value == null) {
            return null;
        }

        return Integer.parseInt(value);
    }


    /**
     * Sets the header defining RBAC roles used for this object.
     *
     * @param roles roles associated with object
     */
    public void setRoles(final Set<String> roles) {
        Objects.requireNonNull(roles, "Roles must be present");

        /* Set roles as a single HTTP header with each role delimited by
         * a comma.
         */
        set(HTTP_ROLE_TAG, MantaUtils.asString(roles));
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
        final String size = wrappedHeaders.getFirstHeaderStringValue("result-set-size");

        if (size == null) {
            return null;
        } else {
            try {
                return Long.parseLong(size);
            } catch (NumberFormatException e) {
                String msg = String.format("Error parsing result-set-size header "
                        + "as long. Actual value: %s", size);
                LoggerFactory.getLogger(getClass()).warn(msg, e);
                return null;
            }
        }
    }

    /**
     * Returns the first {@code "Accept"} header or {@code null} for none.
     *
     * @return {@code "Accept"} header value as a {@code java.lang.String} value
     */
    public String getAccept() {
        return wrappedHeaders.getAccept();
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
        wrappedHeaders.setAccept(accept);
        return this;
    }


    /**
     * Returns the first {@code "Accept-Encoding"} header or {@code null} for none.
     *
     * @return {@code "Accept-Encoding"} header value as a {@code java.lang.String} value
     */
    public String getAcceptEncoding() {
        return wrappedHeaders.getAcceptEncoding();
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
        wrappedHeaders.setAcceptEncoding(acceptEncoding);
        return this;
    }


    /**
     * Returns the first {@code "Authorization"} header or {@code null} for none.
     *
     * @return {@code "Authorization"} header value as a {@code java.lang.String} value
     */
    public String getAuthorization() {
        return wrappedHeaders.getAuthorization();
    }


    /**
     * Returns all {@code "Authorization"} headers or {@code null} for none.
     *
     * @return {@code "Authorization"} headers as a {@code java.util.List} of {@code java.lang.String} values.
     */
    public List<String> getAuthorizationAsList() {
        return wrappedHeaders.getAuthorizationAsList();
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
        wrappedHeaders.setAuthorization(authorization);
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
        wrappedHeaders.setAuthorization(authorization);
        return this;
    }


    /**
     * Returns the first {@code "Cache-Control"} header or {@code null} for none.
     *
     * @return {@code "Cache-Control"} header value as a {@code java.lang.String} value
     */
    public String getCacheControl() {
        return wrappedHeaders.getCacheControl();
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
        wrappedHeaders.setCacheControl(cacheControl);
        return this;
    }


    /**
     * Returns the first {@code "Content-Encoding"} header or {@code null} for none.
     *
     * @return {@code "Content-Encoding"} header value as a {@code java.lang.String} value
     */
    public String getContentEncoding() {
        return wrappedHeaders.getContentEncoding();
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
    public MantaHttpHeaders setContentEncoding(final String contentEncoding) {
        wrappedHeaders.setContentEncoding(contentEncoding);
        return this;
    }


    /**
     * Returns the first {@code "Content-Length"} header or {@code null} for none.
     *
     * @return {@code "Content-Length"} header value as a {@code java.lang.Long} value
     */
    public Long getContentLength() {
        return wrappedHeaders.getContentLength();
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
    public MantaHttpHeaders setContentLength(final Long contentLength) {
        wrappedHeaders.setContentLength(contentLength);
        return this;
    }


    /**
     * Returns the first {@code "Content-MD5"} header or {@code null} for none.
     *
     * @return {@code "Content-MD5"} header value as a {@code java.lang.String} value
     */
    public String getContentMD5() {
        return wrappedHeaders.getContentMD5();
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
        wrappedHeaders.setContentMD5(contentMD5);
        return this;
    }


    /**
     * Returns the first {@code "Content-Range"} header or {@code null} for none.
     *
     * @return {@code "Content-Range"} header value as a {@code java.lang.String} value
     */
    public String getContentRange() {
        return wrappedHeaders.getContentRange();
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
        wrappedHeaders.setContentRange(contentRange);
        return this;
    }


    /**
     * Returns the first {@code "Content-Type"} header or {@code null} for none.
     *
     * @return {@code "Content-Type"} header value as a {@code java.lang.String} value
     */
    public String getContentType() {
        return wrappedHeaders.getContentType();
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
        wrappedHeaders.setContentType(contentType);
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
    public String getCookie() {
        return wrappedHeaders.getCookie();
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
    public MantaHttpHeaders setCookie(final String cookie) {
        wrappedHeaders.setCookie(cookie);
        return this;
    }


    /**
     * Returns the first {@code "Date"} header or {@code null} for none.
     *
     * @return {@code "Date"} header value as a {@code java.lang.String} value
     */
    public String getDate() {
        return wrappedHeaders.getDate();
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
        wrappedHeaders.setDate(date);
        return this;
    }


    /**
     * Returns the first {@code "ETag"} header or {@code null} for none.
     *
     * @return {@code "ETag"} header value as a {@code java.lang.String} value
     */
    public String getETag() {
        return wrappedHeaders.getETag();
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
    public MantaHttpHeaders setETag(final String etag) {
        wrappedHeaders.setETag(etag);
        return this;
    }


    /**
     * Returns the first {@code "Expires"} header or {@code null} for none.
     *
     * @return {@code "Expires"} header value as a {@code java.lang.String} value
     */
    public String getExpires() {
        return wrappedHeaders.getExpires();
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
        wrappedHeaders.setExpires(expires);
        return this;
    }


    /**
     * Returns the first {@code "If-Modified-Since"} header or {@code null} for none.
     *
     * @return {@code "If-Modified-Since"} header value as a {@code java.lang.String} value
     */
    public String getIfModifiedSince() {
        return wrappedHeaders.getIfModifiedSince();
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
        wrappedHeaders.setIfModifiedSince(ifModifiedSince);
        return this;
    }


    /**
     * Returns the first {@code "If-Match"} header or {@code null} for none.
     *
     * @return {@code "If-Match"} header value as a {@code java.lang.String} value
     */
    public String getIfMatch() {
        return wrappedHeaders.getIfMatch();
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
        wrappedHeaders.setIfMatch(ifMatch);
        return this;
    }


    /**
     * Returns the first {@code "If-None-Match"} header or {@code null} for none.
     *
     * @return {@code "If-None-Match"} header value as a {@code java.lang.String} value
     */
    public String getIfNoneMatch() {
        return wrappedHeaders.getIfNoneMatch();
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
        wrappedHeaders.setIfNoneMatch(ifNoneMatch);
        return this;
    }


    /**
     * Returns the first {@code "If-Unmodified-Since"} header or {@code null} for none.
     *
     * @return {@code "If-Unmodified-Since"} header value as a {@code java.lang.String} value
     */
    public String getIfUnmodifiedSince() {
        return wrappedHeaders.getIfUnmodifiedSince();
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
        wrappedHeaders.setIfUnmodifiedSince(ifUnmodifiedSince);
        return this;
    }


    /**
     * Returns the first {@code "If-Range"} header or {@code null} for none.
     *
     * @return {@code "If-Range"} header value as a {@code java.lang.String} value
     */
    public String getIfRange() {
        return wrappedHeaders.getIfRange();
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
        wrappedHeaders.setIfRange(ifRange);
        return this;
    }


    /**
     * Returns the first {@code "Last-Modified"} header or {@code null} for none.
     *
     * @return {@code "Last-Modified"} header value as a {@code java.lang.String} value
     */
    public String getLastModified() {
        return wrappedHeaders.getLastModified();
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
    public MantaHttpHeaders setLastModified(final String lastModified) {
        wrappedHeaders.setLastModified(lastModified);
        return this;
    }


    /**
     * Returns the first {@code "Location"} header or {@code null} for none.
     *
     * @return {@code "Location"} header value as a {@code java.lang.String} value
     */
    public String getLocation() {
        return wrappedHeaders.getLocation();
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
        wrappedHeaders.setLocation(location);
        return this;
    }


    /**
     * Returns the first {@code "MIME-Version"} header or {@code null} for none.
     *
     * @return {@code "MIME-Version"} header value as a {@code java.lang.String} value
     */
    public String getMimeVersion() {
        return wrappedHeaders.getMimeVersion();
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
    public MantaHttpHeaders setMimeVersion(final String mimeVersion) {
        wrappedHeaders.setMimeVersion(mimeVersion);
        return this;
    }


    /**
     * Returns the first {@code "Range"} header or {@code null} for none.
     *
     * @return {@code "Range"} header value as a {@code java.lang.String} value
     */
    public String getRange() {
        return wrappedHeaders.getRange();
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
        wrappedHeaders.setRange(range);
        return this;
    }


    /**
     * Returns the first {@code "Retry-After"} header or {@code null} for none.
     *
     * @return {@code "Retry-After"} header value as a {@code java.lang.String} value
     */
    public String getRetryAfter() {
        return wrappedHeaders.getRetryAfter();
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
        wrappedHeaders.setRetryAfter(retryAfter);
        return this;
    }


    /**
     * Returns the first {@code "User-Agent"} header or {@code null} for none.
     *
     * @return {@code "User-Agent"} header value as a {@code java.lang.String} value
     */
    public String getUserAgent() {
        return wrappedHeaders.getUserAgent();
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
        wrappedHeaders.setUserAgent(userAgent);
        return this;
    }


    /**
     * Returns the first {@code "WWW-Authenticate"} header or {@code null} for none.
     *
     * @return {@code "WWW-Authenticate"} header value as a {@code java.lang.String} value
     */
    public String getAuthenticate() {
        return wrappedHeaders.getAuthenticate();
    }


    /**
     * Returns all {@code "WWW-Authenticate"} headers or {@code null} for none.
     *
     * @return {@code "WWW-Authenticate"} headers as a {@code java.util.List} of {@code java.lang.String} values
     */
    public List<String> getAuthenticateAsList() {
        return wrappedHeaders.getAuthenticateAsList();
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
        wrappedHeaders.setAuthenticate(authenticate);
        return this;
    }


    /**
     * Returns the first {@code "Age"} header or {@code null} for none.
     *
     * @return {@code "Age"} header value as a {@code java.lang.Long} value
     */
    public Long getAge() {
        return wrappedHeaders.getAge();
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
        wrappedHeaders.setAge(age);
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
    public MantaHttpHeaders setBasicAuthentication(final String username, final String password) {
        wrappedHeaders.setBasicAuthentication(username, password);
        return this;
    }


    /**
     * Returns the first header string value for the given header name.
     *
     * @param name header name (may be any case)
     * @return first header string value or {@code null} if not found
     */
    public String getFirstHeaderStringValue(final String name) {
        return wrappedHeaders.getFirstHeaderStringValue(name);
    }


    /**
     * Returns an unmodifiable list of the header string values for the given header name.
     *
     * @param name header name (may be any case)
     * @return header string values or empty if not found
     */
    public List<String> getHeaderStringValues(final String name) {
        return wrappedHeaders.getHeaderStringValues(name);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#get}
     */
    public Object get(final Object name) {
        return wrappedHeaders.get(name);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#get}
     *
     * @return the value serialized to a {@code java.lang.String}
     */
    public String getAsString(final Object name) {
        return MantaUtils.asString(wrappedHeaders.get(name));
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#put}
     */
    public Object put(final String fieldName, final Object value) {
        return wrappedHeaders.put(fieldName, value);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#putAll}
     */
    public void putAll(final Map<? extends String, ?> map) {
        wrappedHeaders.putAll(map);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#remove}
     */
    public Object remove(final Object name) {
        return wrappedHeaders.remove(name);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.AbstractMap#entrySet}
     */
    public Set<Map.Entry<String, Object>> entrySet() {
        return wrappedHeaders.entrySet();
    }


    /**
     * Returns the map of unknown data key name to value.
     *
     * @return {@code java.util.Map} of unknown key-value mappings.
     */
    public Map<String, Object> getUnknownKeys() {
        return wrappedHeaders.getUnknownKeys();
    }


    /**
     * Sets the map of unknown data key name to value.
     *
     * @param unknownFields {@code java.util.Map} of unknown key-value mappings
     */
    public void setUnknownKeys(final Map<String, Object> unknownFields) {
        wrappedHeaders.setUnknownKeys(unknownFields);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#entrySet}
     */
    public int size() {
        return wrappedHeaders.size();
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#isEmpty}
     */
    public boolean isEmpty() {
        return wrappedHeaders.isEmpty();
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#containsValue}
     */
    public boolean containsValue(final Object value) {
        return wrappedHeaders.containsValue(value);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#containsKey}
     */
    public boolean containsKey(final Object key) {
        return wrappedHeaders.containsKey(key);
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#clear}
     */
    public void clear() {
        wrappedHeaders.clear();
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#keySet}
     */
    public Set<String> keySet() {
        return wrappedHeaders.keySet();
    }


    /**
     * {@inheritDoc}
     * {@link java.util.Map#values}
     */
    public Collection<Object> values() {
        return wrappedHeaders.values();
    }


    /**
     * Sets the given field value (may be {@code null}) for the given field name. Any existing value
     * for the field will be overwritten. It may be more slightly more efficient than
     * {@link #put(String, Object)} because it avoids accessing the field's original value.
     *
     * <p>
     * Overriding is only supported for the purpose of calling the super implementation and changing
     * the return type, but nothing else.
     * </p>
     *
     * @param fieldName field name of the header
     * @param value value for the header
     * @return this instance
     */
    public MantaHttpHeaders set(final String fieldName, final Object value) {
        wrappedHeaders.set(fieldName, value);
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
        return "MantaHttpHeaders{"
                + "wrappedHeaders="
                + wrappedHeaders
                + '}';
    }
}
