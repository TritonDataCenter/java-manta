package com.joyent.manta.http;

import com.joyent.manta.http.HttpRange.Request;
import com.joyent.manta.http.HttpRange.Response;
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
public class ResumableDownloadMarkerTest {

    public void ctorRejectsInvalidInputs() {
        final Response validRange = new Response(0, 1, 2L);

        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, null));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker(null, validRange));
        assertThrows(IllegalStateException.class, () -> new ResumableDownloadMarker("", validRange));
        assertThrows(NullPointerException.class, () -> new ResumableDownloadMarker("A", null));
    }

    private static final String STUB_ETAG = "abc";

    public static final Request STUB_REQUEST_RANGE = new Request(0, 3);

    private static final Response STUB_CONTENT_RANGE = new Response(0, 3, 4L);

    public void canValidateResponses() throws HttpException {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

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
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

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
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // shouldn't read a negative number of bytes
        marker.updateRangeStart(-1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsOnMoreBytesThanExpected() {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        // shouldn't be able to read more bytes than the total object size
        marker.updateRangeStart(10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsWhenBytesReadDecreases() {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);
        // advance marker by pretending we read two bytes
        marker.updateRangeStart(2);

        // can't have read less bytes than we did at last update
        marker.updateRangeStart(1);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void validatesBytesReadThrowsWhenSettingRangeStartToContentLength() {
        final ResumableDownloadMarker marker = new ResumableDownloadMarker(STUB_ETAG, STUB_CONTENT_RANGE);

        marker.updateRangeStart(STUB_CONTENT_RANGE.contentLength());
    }

    public void markersAreCreatedByValidatingTheInitialExchangePlainRequest() throws Exception {
        assertNotNull(
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                                SC_OK,
                                                                ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE)));
    }

    public void markersAreCreatedByValidatingTheInitialExchangeRangeRequest() throws Exception {
        assertNotNull(
                ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_REQUEST_RANGE),
                                                                SC_PARTIAL_CONTENT,
                                                                ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE)));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseIsMissingETagWithPlainRequest() throws Exception {
        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                        SC_OK,
                                                        ImmutablePair.of(null, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseIsMissingETagWithRangeRequest() throws Exception {
        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_REQUEST_RANGE),
                                                        SC_PARTIAL_CONTENT,
                                                        ImmutablePair.of(null, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectETag() throws Exception {
        // invalid ETag
        final String badEtag = STUB_ETAG.substring(1);
        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                        SC_OK,
                                                        ImmutablePair.of(badEtag, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectContentRange() throws Exception {
        final HttpRange.Response badContentRange = new HttpRange.Response(0, 99, 100);

        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_REQUEST_RANGE),
                                                        SC_PARTIAL_CONTENT,
                                                        ImmutablePair.of(STUB_ETAG, badContentRange));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectResponseCodeForPlainRequest() throws Exception {
        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, null),
                                                        SC_PARTIAL_CONTENT,
                                                        ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE));
    }

    @Test(expectedExceptions = ProtocolException.class)
    public void throwsWhenResponseHasIncorrectResponseCodeForRangeRequest() throws Exception {
        ResumableDownloadMarker.validateInitialExchange(ImmutablePair.of(STUB_ETAG, STUB_REQUEST_RANGE),
                                                        SC_OK,
                                                        ImmutablePair.of(STUB_ETAG, STUB_CONTENT_RANGE));
    }

    public void usefulToString() {
        final Response STUB_FULL_RANGE = new Response(0, 3, 4L);
        final String etag = new RandomStringGenerator.Builder().withinRange('a', 'z').build().generate(10);

        final String rangeRgx = StringUtils.join(
                new Object[]{"", STUB_FULL_RANGE.getStartInclusive(), STUB_FULL_RANGE.getEndInclusive(), ""},
                ".*");

        final String marker = new ResumableDownloadMarker(etag, STUB_FULL_RANGE).toString();

        assertTrue(marker.contains(etag));
        assertTrue(marker.matches(rangeRgx));
    }
}
