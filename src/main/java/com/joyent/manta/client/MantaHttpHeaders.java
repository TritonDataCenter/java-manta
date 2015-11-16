package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.FieldInfo;
import com.google.api.client.util.Types;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.message.BasicHeader;

import java.util.*;

import static com.joyent.http.signature.HttpSignerUtils.X_REQUEST_ID_HEADER;

/**
 * Object encapsulating the HTTP headers to be sent to the Manta API.
 * When non-standard HTTP headers are used as part of a PUT request to
 * Manta, they are stored as metadata about an object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaHttpHeaders {
    private final HttpHeaders wrappedHeaders = new HttpHeaders();

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
     * {@link MantaObject} instance
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
                    final Header header = new BasicHeader(displayName, asString(multiple));
                    headers.add(header);
                }
            } else {
                final Header header = new BasicHeader(displayName, asString(value));
                headers.add(header);
            }
        }

        Header[] array = new Header[headers.size()];
        headers.toArray(array);

        return array;
    }

    HttpHeaders asGoogleClientHttpHeaders() {
        return wrappedHeaders;
    }

    private static String asString(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Enum<?>) {
            return FieldInfo.of((Enum<?>) value).getName();
        } else if (value instanceof Iterable<?>) {
            StringBuilder sb = new StringBuilder();

            Iterator<?> itr = ((Iterable<?>) value).iterator();
            while (itr.hasNext()) {
                Object next = itr.next();

                if (next != null) {
                    sb.append(next.toString());
                }

                if (itr.hasNext()) {
                    sb.append(",");
                }
            }

            return sb.toString();
        } else if (value.getClass().isArray()) {
            Object[] array = (Object[])value;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < array.length; i++) {
                Object next = array[i];

                if (next != null) {
                    sb.append(next.toString());
                }

                if (i < array.length - 1) {
                    sb.append("; ");
                }
            }

            return sb.toString();
        }

        return value.toString();
    }

    public Map<String, ?> metadata() {
        final Map<String, Object> metadata = new HashMap<>();
        for (Map.Entry<String, Object> entry : wrappedHeaders.entrySet()) {
            if (entry.getKey().startsWith("m-")) {
                metadata.put(entry.getKey(), entry.getValue());
            }
        }

        return metadata;
    }

    public Map<String, String> metadataAsStrings() {
        final Map<String, String> metadata = new HashMap<>();
        for (Map.Entry<String, Object> entry : wrappedHeaders.entrySet()) {
            if (entry.getKey().startsWith("m-")) {
                metadata.put(entry.getKey(), asString(entry.getValue()));
            }
        }

        return metadata;
    }

    public void putAllMetadata(final MantaMetadata metadata) {
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

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

    public String getAccept() {
        return wrappedHeaders.getAccept();
    }

    public MantaHttpHeaders setAccept(String accept) {
        wrappedHeaders.setAccept(accept);
        return this;
    }

    public String getAcceptEncoding() {
        return wrappedHeaders.getAcceptEncoding();
    }

    public MantaHttpHeaders setAcceptEncoding(String acceptEncoding) {
        wrappedHeaders.setAcceptEncoding(acceptEncoding);
        return this;
    }

    public String getAuthorization() {
        return wrappedHeaders.getAuthorization();
    }

    public List<String> getAuthorizationAsList() {
        return wrappedHeaders.getAuthorizationAsList();
    }

    public MantaHttpHeaders setAuthorization(String authorization) {
        wrappedHeaders.setAuthorization(authorization);
        return this;
    }

    public MantaHttpHeaders setAuthorization(List<String> authorization) {
        wrappedHeaders.setAuthorization(authorization);
        return this;
    }

    public String getCacheControl() {
        return wrappedHeaders.getCacheControl();
    }

    public MantaHttpHeaders setCacheControl(String cacheControl) {
        wrappedHeaders.setCacheControl(cacheControl);
        return this;
    }

    public String getContentEncoding() {
        return wrappedHeaders.getContentEncoding();
    }

    public MantaHttpHeaders setContentEncoding(String contentEncoding) {
        wrappedHeaders.setContentEncoding(contentEncoding);
        return this;
    }

    public Long getContentLength() {
        return wrappedHeaders.getContentLength();
    }

    public MantaHttpHeaders setContentLength(Long contentLength) {
        wrappedHeaders.setContentLength(contentLength);
        return this;
    }

    public String getContentMD5() {
        return wrappedHeaders.getContentMD5();
    }

    public MantaHttpHeaders setContentMD5(String contentMD5) {
        wrappedHeaders.setContentMD5(contentMD5);
        return this;
    }

    public String getContentRange() {
        return wrappedHeaders.getContentRange();
    }

    public MantaHttpHeaders setContentRange(String contentRange) {
        wrappedHeaders.setContentRange(contentRange);
        return this;
    }

    public String getContentType() {
        return wrappedHeaders.getContentType();
    }

    public MantaHttpHeaders setContentType(String contentType) {
        wrappedHeaders.setContentType(contentType);
        return this;
    }

    public String getCookie() {
        return wrappedHeaders.getCookie();
    }

    public MantaHttpHeaders setCookie(String cookie) {
        wrappedHeaders.setCookie(cookie);
        return this;
    }

    public String getDate() {
        return wrappedHeaders.getDate();
    }

    public MantaHttpHeaders setDate(String date) {
        wrappedHeaders.setDate(date);
        return this;
    }

    public String getETag() {
        return wrappedHeaders.getETag();
    }

    public MantaHttpHeaders setETag(String etag) {
        wrappedHeaders.setETag(etag);
        return this;
    }

    public String getExpires() {
        return wrappedHeaders.getExpires();
    }

    public MantaHttpHeaders setExpires(String expires) {
        wrappedHeaders.setExpires(expires);
        return this;
    }

    public String getIfModifiedSince() {
        return wrappedHeaders.getIfModifiedSince();
    }

    public MantaHttpHeaders setIfModifiedSince(String ifModifiedSince) {
        wrappedHeaders.setIfModifiedSince(ifModifiedSince);
        return this;
    }

    public String getIfMatch() {
        return wrappedHeaders.getIfMatch();
    }

    public MantaHttpHeaders setIfMatch(String ifMatch) {
        wrappedHeaders.setIfMatch(ifMatch);
        return this;
    }

    public String getIfNoneMatch() {
        return wrappedHeaders.getIfNoneMatch();
    }

    public MantaHttpHeaders setIfNoneMatch(String ifNoneMatch) {
        wrappedHeaders.setIfNoneMatch(ifNoneMatch);
        return this;
    }

    public String getIfUnmodifiedSince() {
        return wrappedHeaders.getIfUnmodifiedSince();
    }

    public MantaHttpHeaders setIfUnmodifiedSince(String ifUnmodifiedSince) {
        wrappedHeaders.setIfUnmodifiedSince(ifUnmodifiedSince);
        return this;
    }

    public String getIfRange() {
        return wrappedHeaders.getIfRange();
    }

    public MantaHttpHeaders setIfRange(String ifRange) {
        wrappedHeaders.setIfRange(ifRange);
        return this;
    }

    public String getLastModified() {
        return wrappedHeaders.getLastModified();
    }

    public MantaHttpHeaders setLastModified(String lastModified) {
        wrappedHeaders.setLastModified(lastModified);
        return this;
    }

    public String getLocation() {
        return wrappedHeaders.getLocation();
    }

    public MantaHttpHeaders setLocation(String location) {
        wrappedHeaders.setLocation(location);
        return this;
    }

    public String getMimeVersion() {
        return wrappedHeaders.getMimeVersion();
    }

    public MantaHttpHeaders setMimeVersion(String mimeVersion) {
        wrappedHeaders.setMimeVersion(mimeVersion);
        return this;
    }

    public String getRange() {
        return wrappedHeaders.getRange();
    }

    public MantaHttpHeaders setRange(String range) {
        wrappedHeaders.setRange(range);
        return this;
    }

    public String getRetryAfter() {
        return wrappedHeaders.getRetryAfter();
    }

    public MantaHttpHeaders setRetryAfter(String retryAfter) {
        wrappedHeaders.setRetryAfter(retryAfter);
        return this;
    }

    public String getUserAgent() {
        return wrappedHeaders.getUserAgent();
    }

    public MantaHttpHeaders setUserAgent(String userAgent) {
        wrappedHeaders.setUserAgent(userAgent);
        return this;
    }

    public String getAuthenticate() {
        return wrappedHeaders.getAuthenticate();
    }

    public List<String> getAuthenticateAsList() {
        return wrappedHeaders.getAuthenticateAsList();
    }

    public MantaHttpHeaders setAuthenticate(String authenticate) {
        wrappedHeaders.setAuthenticate(authenticate);
        return this;
    }

    public Long getAge() {
        return wrappedHeaders.getAge();
    }

    public MantaHttpHeaders setAge(Long age) {
        wrappedHeaders.setAge(age);
        return this;
    }

    public MantaHttpHeaders setBasicAuthentication(String username, String password) {
        wrappedHeaders.setBasicAuthentication(username, password);
        return this;
    }

    public String getFirstHeaderStringValue(String name) {
        return wrappedHeaders.getFirstHeaderStringValue(name);
    }

    public List<String> getHeaderStringValues(String name) {
        return wrappedHeaders.getHeaderStringValues(name);
    }

    public Object get(Object name) {
        return wrappedHeaders.get(name);
    }

    public String getAsString(Object name) {
        return asString(wrappedHeaders.get(name));
    }

    public Object put(String fieldName, Object value) {
        return wrappedHeaders.put(fieldName, value);
    }

    public void putAll(Map<? extends String, ?> map) {
        wrappedHeaders.putAll(map);
    }

    public Object remove(Object name) {
        return wrappedHeaders.remove(name);
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return wrappedHeaders.entrySet();
    }

    public Map<String, Object> getUnknownKeys() {
        return wrappedHeaders.getUnknownKeys();
    }

    public void setUnknownKeys(Map<String, Object> unknownFields) {
        wrappedHeaders.setUnknownKeys(unknownFields);
    }

    public int size() {
        return wrappedHeaders.size();
    }

    public boolean isEmpty() {
        return wrappedHeaders.isEmpty();
    }

    public boolean containsValue(Object value) {
        return wrappedHeaders.containsValue(value);
    }

    public boolean containsKey(Object key) {
        return wrappedHeaders.containsKey(key);
    }

    public void clear() {
        wrappedHeaders.clear();
    }

    public Set<String> keySet() {
        return wrappedHeaders.keySet();
    }

    public Collection<Object> values() {
        return wrappedHeaders.values();
    }

    public MantaHttpHeaders set(String fieldName, Object value) {
        wrappedHeaders.set(fieldName, value);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MantaHttpHeaders headers = (MantaHttpHeaders) o;
        return Objects.equals(wrappedHeaders, headers.wrappedHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrappedHeaders);
    }

    @Override
    public String toString() {
        return "MantaHttpHeaders{" +
                "wrappedHeaders=" + wrappedHeaders +
                '}';
    }
}
