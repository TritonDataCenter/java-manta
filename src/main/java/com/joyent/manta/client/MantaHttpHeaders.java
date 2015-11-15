package com.joyent.manta.client;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.util.FieldInfo;
import com.google.api.client.util.Types;
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BufferedHeader;
import org.apache.http.protocol.HTTP;

import java.util.*;

import static com.joyent.http.signature.HttpSignerUtils.X_REQUEST_ID_HEADER;

/**
 * Object encapsulating the HTTP headers to be sent to the Manta API.
 * When non-standard HTTP headers are used as part of a PUT request to
 * Manta, they are stored as metadata about an object.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
public class MantaHttpHeaders extends HttpHeaders {
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
        putAll(headers);
    }

    /**
     * Creates an instance with headers prepopulated from an existing
     * {@link MantaObject} instance
     *
     * @param mantaObject Manta object to read headers from
     */
    public MantaHttpHeaders(final MantaObject mantaObject) {
        this(mantaObject.getHttpHeaders());
    }


    /**
     * Creates an instance with headers prepopulated from the Google HTTP Client
     * headers class.
     *
     * @param headers headers to prepopulate
     */
    MantaHttpHeaders(final HttpHeaders headers) {
        if (headers != null) {
            fromHttpHeaders(headers);
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
                put(header.getName(), null);
            }

            switch (header.getName().toLowerCase()) {
                case "content-length":
                    setContentLength(Long.parseLong(header.getValue()));
                    break;
                case "age":
                    setAge(Long.parseLong(header.getValue()));
                    break;
                default:
                    List<String> values = new ArrayList<>();
                    for (HeaderElement e : header.getElements()) {
                        values.add(e.getValue());
                    }
                    set(header.getName(), values);
            }
        }
    }


    Header[] asApacheHttpHeaders() {
        final ArrayList<Header> headers = new ArrayList<>();

        for (Map.Entry<String, ?> entry : getUnknownKeys().entrySet()) {
            final String name = entry.getKey();
            final Object value = entry.getValue();

            final String displayName;
            FieldInfo fieldInfo = this.getClassInfo().getFieldInfo(name);
            if (fieldInfo != null) {
                displayName = fieldInfo.getName();
            } else {
                displayName = name;
            }

            if (value == null) {
                headers.add(new BasicHeader(displayName, null));
                continue;
            }

            final Class<? extends Object> valueClass = value.getClass();

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

    private static String asString(Object value) {
        if (value instanceof Enum<?>) {
            return FieldInfo.of((Enum<?>) value).getName();
        }

        return value.toString();
    }

    public String getRequestId() {
        Object requestId = get(X_REQUEST_ID_HEADER);
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
}
