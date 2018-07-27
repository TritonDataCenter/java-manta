package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.BoundedRequest;
import com.joyent.manta.http.HttpRange.Response;
import com.joyent.manta.http.HttpRange.UnboundedRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.text.RandomStringGenerator;
import org.apache.http.HttpException;
import org.apache.http.ProtocolException;
import org.testng.annotations.Test;

import static org.apache.http.HttpStatus.SC_OK;
import static org.apache.http.HttpStatus.SC_PARTIAL_CONTENT;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test
public class HttpDownloadContinuationMarkerTest {

    public void ctorRejectsInvalidInputs() {
        final Response validRange = new Response(0, 1, 2L);

        assertThrows(IllegalStateException.class, () -> new HttpDownloadContinuationMarker(null, null));
        assertThrows(IllegalStateException.class, () -> new HttpDownloadContinuationMarker(null, validRange));
        assertThrows(IllegalStateException.class, () -> new HttpDownloadContinuationMarker("", validRange));
        assertThrows(NullPointerException.class, () -> new HttpDownloadContinuationMarker("A", null));
    }

    private static final String STUB_ETAG = "abc";

    private static final UnboundedRequest STUB_UNBOUNDED_REQUEST_RANGE = new UnboundedRequest(0);

    private static final BoundedRequest STUB_BOUNDED_REQUEST_RANGE = new BoundedRequest(0, 3);

    private static final Response STUB_CONTENT_RANGE = new Response(0, 3, 4L);

    public void canValidateResponses() throws HttpException {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // pretend there was an error receiving headers and we had to restart the initial request completely
        marker.validateResponseRange(STUB_CONTENT_RANGE);

        // pretend we got the first byte
        marker.updateRangeStart(1);

        final Response validPartialRange = new Response(1, 3, 4L);
        marker.validateResponseRange(validPartialRange);

        final Response invalidEndRange = new Response(0, 2, 4L);
        assertThrows(HttpException.class, () -> marker.validateResponseRange(invalidEndRange));

        // getting bytes we've already gotten is also bad
        assertThrows(HttpException.class, () -> marker.validateResponseRange(STUB_CONTENT_RANGE));
    }

    public void validatesBytesReadCanProceedNormally() {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // it's possible to encounter an error without reading any bytes
        marker.updateRangeStart(0);

        // normal updates
        marker.updateRangeStart(1);

        marker.updateRangeStart(2);
        marker.updateRangeStart(2); // failed to proceed

        marker.updateRangeStart(STUB_CONTENT_RANGE.getEndInclusive());
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsOnNegativeBytes() {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // shouldn't read a negative number of bytes
        marker.updateRangeStart(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsOnMoreBytesThanExpected() {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // shouldn't be able to read more bytes than the total object size
        marker.updateRangeStart(10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsWhenBytesReadDecreases() {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);
        // advance marker by pretending we read two bytes
        marker.updateRangeStart(2);

        // can't have read less bytes than we did at last update
        marker.updateRangeStart(1);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsWhenSettingRangeStartToContentLength() {
        final HttpDownloadContinuationMarker marker = new HttpDownloadContinuationMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        marker.updateRangeStart(STUB_CONTENT_RANGE.contentLength());
    }

    public void markersAreCreatedByValidatingTheInitialExchangePlainRequest() throws Exception {
        assertNotNull(
                HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                                       SC_OK,
                                                                       ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE)));
    }

    public void markersAreCreatedByValidatingTheInitialExchangeRangeRequest() throws Exception {
        assertNotNull(
                HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_BOUNDED_REQUEST_RANGE),
                                                                       SC_PARTIAL_CONTENT,
                                                                       ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE)));
    }

    public void initialRequestValidationAllowsUnboundedRangeRequest() throws Exception {
        assertNotNull(
                HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_UNBOUNDED_REQUEST_RANGE),
                                                                       SC_PARTIAL_CONTENT,
                                                                       ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE)));

    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseIsMissingETagWithPlainRequest() throws Exception {
        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                               SC_OK,
                                                               ImmutablePair.of(null, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseIsMissingETagWithRangeRequest() throws Exception {
        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_BOUNDED_REQUEST_RANGE),
                                                               SC_PARTIAL_CONTENT,
                                                               ImmutablePair.of(null, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectETag() throws Exception {
        final String badEtag = STUB_ETAG.substring(1);
        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                               SC_OK,
                                                               ImmutablePair.of(badEtag, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectContentRange() throws Exception {
        final HttpRange.Response badContentRange = new HttpRange.Response(0, 99, 100);

        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_BOUNDED_REQUEST_RANGE),
                                                               SC_PARTIAL_CONTENT,
                                                               ImmutablePair.of(STUB_ETAG, badContentRange));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectResponseCodeForPlainRequest() throws Exception {
        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                               SC_PARTIAL_CONTENT,
                                                               ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectResponseCodeForRangeRequest() throws Exception {
        HttpDownloadContinuationMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_BOUNDED_REQUEST_RANGE),
                                                               SC_OK,
                                                               ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE));
    }

    public void usefulToString() {
        final Response STUB_FULL_RANGE = new Response(0, 3, 4L);
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final String rangeRgx = StringUtils.join(
                new Object[]{"", STUB_FULL_RANGE.getStartInclusive(), STUB_FULL_RANGE.getEndInclusive(), ""},
                ".*");

        final String marker = new HttpDownloadContinuationMarker(etag, STUB_FULL_RANGE).toString();

        assertTrue(marker.contains(etag));
        assertTrue(marker.matches(rangeRgx));
    }
}
