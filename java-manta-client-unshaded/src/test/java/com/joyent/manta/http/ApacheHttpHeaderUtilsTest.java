package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.BoundedRequest;
import org.apache.http.Header;
import org.apache.http.ProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.message.BasicHeader;
import org.testng.annotations.Test;

import java.util.Map;

import static com.joyent.manta.http.ApacheHttpHeaderUtils.extractDownloadRequestFingerprint;
import static com.joyent.manta.http.ApacheHttpTestUtils.prepareRequestWithHeaders;
import static com.joyent.manta.http.ApacheHttpTestUtils.singleValueHeaderList;
import static com.joyent.manta.util.MantaUtils.unmodifiableMap;
import static java.util.Collections.emptyMap;
import static org.apache.http.HttpHeaders.IF_MATCH;
import static org.apache.http.HttpHeaders.RANGE;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;

@Test
public class ApacheHttpHeaderUtilsTest {

    public void validatesNoCollidingHeadersForInitialRequest() throws Exception {
        validatesCompatibleHeadersForInitialRequest(emptyMap());
    }

    public void validatesCompatibleIfMatchHeaderForInitialRequest() throws Exception {
        final String ifMatchHeaderValue = new BoundedRequest(0, 10).render();

        // users are allowed to omit or set their own if-match header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(IF_MATCH, singleValueHeaderList(IF_MATCH, ifMatchHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(IF_MATCH, new Header[]{null}));
        });

        // the following is most likely user error, though we should ideally disallow it
        // (if-match: "" doesn't make sense for GET, only for PUT, and this is all about resumable *downloads*)
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "")));
        });

        // we make no effort to merge headers
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(
                            IF_MATCH, new Header[]{new BasicHeader(IF_MATCH, ifMatchHeaderValue), new BasicHeader(
                                    IF_MATCH, ifMatchHeaderValue)}));
        });
    }

    public void validatesCompatibleRangeHeaderForInitialRequest() throws Exception {
        final String rangeHeaderValue = new BoundedRequest(0, 10).render();

        // users are allowed to omit or set their own initial range header
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, new Header[]{}));
        validatesCompatibleHeadersForInitialRequest(
                unmodifiableMap(RANGE, singleValueHeaderList(RANGE, rangeHeaderValue)));

        // this should never occur so we consider it programmer error, something has been subclassed incorrectly
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, new Header[]{null}));
        });

        // the following are most likely user error
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "")));
        });

        final ProtocolException malformed = expectThrows(
                ProtocolException.class, () -> {
                    validatesCompatibleHeadersForInitialRequest(
                            unmodifiableMap(RANGE, singleValueHeaderList(RANGE, "duck")));
                });

        assertTrue(malformed.getMessage().contains("duck"));

        // we explicitly do not handle resuming multiple ranges (though we could in the future)
        // (supplying the same range header twice is also an error, but we're not concerned with that)
        assertThrows(ProtocolException.class, () -> {
            validatesCompatibleHeadersForInitialRequest(
                    unmodifiableMap(
                            RANGE, new Header[]{new BasicHeader(RANGE, rangeHeaderValue), new BasicHeader(
                                    RANGE,
                                    rangeHeaderValue)}));
        });
    }

    public void complainsAboutBothHeadersWhenRangeAndIfMatchAreMalformed() throws Exception {
        final ProtocolException e = expectThrows(
                ProtocolException.class, () -> {
                    validatesCompatibleHeadersForInitialRequest(
                            unmodifiableMap(
                                    RANGE, singleValueHeaderList(RANGE, ""),
                                    IF_MATCH, singleValueHeaderList(IF_MATCH, "")));
                });

        assertTrue(e.getMessage().contains(RANGE));
        assertTrue(e.getMessage().contains(IF_MATCH));
    }

    private static void validatesCompatibleHeadersForInitialRequest(final Map<String, Header[]> headers)
            throws Exception {
        final HttpGet req = prepareRequestWithHeaders(headers);
        verifyNoMoreInteractions(req);

        extractDownloadRequestFingerprint(req);
    }
}
