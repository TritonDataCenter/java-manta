package com.joyent.manta.http;

import com.joyent.manta.exception.MantaResumedDownloadIncompatibleResponseException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Response interceptor which aids in carrying out resumable downloads with the request context's
 * {@link ResumableDownloadCoordinator}. {@link }
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public class ResumableDownloadHttpResponseInterceptor implements HttpResponseInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadHttpResponseInterceptor.class);

    /**
     * Singleton for users providing their own {@link org.apache.http.impl.client.HttpClientBuilder}.
     */
    public static final ResumableDownloadHttpResponseInterceptor INSTANCE =
            new ResumableDownloadHttpResponseInterceptor();

    /**
     * Private constructor since this class is immutable and a singleton instance is provided.
     */
    private ResumableDownloadHttpResponseInterceptor() {
    }

    /**
     * Processes a response before the message body is evaluated.
     *
     * @param response the response to postprocess
     * @param context  the context for the request
     * @throws HttpException in case of an HTTP protocol violation
     * @throws IOException   in case of an I/O error
     */
    @Override
    public void process(final HttpResponse response,
                        final HttpContext context) throws HttpException, IOException {
        final ResumableDownloadCoordinator coordinator = ResumableDownloadCoordinator.extractFromContext(context);
        if (coordinator == null) {
            // for one reason or another this request can't be resumed, abort
            return;
        }

        final ImmutablePair<String, HttpRange.Response> fingerprint =
                ResumableDownloadCoordinator.extractResponseFingerprint(response);

        if (fingerprint == null) {
            // there is not enough information in the response to create or update a marker

            final boolean inProgress = coordinator.inProgress();

            coordinator.cancel();
            if (inProgress) {
                final String message = "Resumed response lacks range info, aborting retry";
                throw new MantaResumedDownloadIncompatibleResponseException(message);
            }

            return;
        }

        if (coordinator.inProgress()) {
            // verify that the returned range matches the marker's current range
            LOG.debug("verifying that returned response matches requested range");
            coordinator.validateResponse(response);
        } else {
            LOG.debug("attaching marker");
            coordinator.attachMarker(new ResumableDownloadMarker(fingerprint.left, fingerprint.right));
        }

    }
}
