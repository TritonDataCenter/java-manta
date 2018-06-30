package com.joyent.manta.http;

import org.apache.http.Header;
import org.apache.http.HttpMessage;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicRequestLine;

import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ApacheHttpTestUtils {

    private ApacheHttpTestUtils() {
    }

    static Header[] singleValueHeaderList(final String name, final String value) {
        return new Header[]{new BasicHeader(name, value)};
    }

    static HttpGet prepareRequestWithHeaders(final Map<String, Header[]> headers) {
        final HttpGet req = prepareMessageWithHeaders(HttpGet.class, headers);

        when(req.getRequestLine()).thenReturn(new BasicRequestLine(HttpGet.METHOD_NAME, "", HttpVersion.HTTP_1_1));

        return req;
    }

    static HttpResponse prepareResponseWithHeaders(final Map<String, Header[]> headers) {
        return prepareMessageWithHeaders(HttpResponse.class, headers);
    }

    static <T extends HttpMessage> T prepareMessageWithHeaders(final Class<T> klass,
                                                               final Map<String, Header[]> headers) {
        final T msg = mock(klass);

        // return an empty list unless a list of headers was provided
        when(msg.getHeaders(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            return headers.getOrDefault(headerName, new Header[0]);
        });

        when(msg.getFirstHeader(anyString())).then(invocation -> {
            final String headerName = invocation.getArgument(0);
            if (!headers.containsKey(headerName)) {
                return null;
            }

            final Header[] matched = headers.get(headerName);
            if (matched.length == 0) {
                return null;
            }

            return matched[0];
        });

        for (final Map.Entry<String, Header[]> headerNameAndList : headers.entrySet()) {
            final String headerName = headerNameAndList.getKey();
            when(msg.getHeaders(headerName)).thenReturn(headerNameAndList.getValue());
        }

        return msg;
    }
}
