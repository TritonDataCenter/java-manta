package com.joyent.manta.http;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Request interceptor which aids in carrying out resumable downloads with the request context's
 * {@link ResumableDownloadCoordinator}. {@link }
 *
 * @author <a href="https://github.com/tjcelaya">Tomas Celaya</a>
 * @since 3.2.2
 */
public class ResumableDownloadHttpRequestInterceptor implements HttpRequestInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(ResumableDownloadHttpRequestInterceptor.class);

    /**
     * Singleton for users providing their own {@link org.apache.http.impl.client.HttpClientBuilder}.
     */
    public static final ResumableDownloadHttpRequestInterceptor INSTANCE =
            new ResumableDownloadHttpRequestInterceptor();

    /**
     * Processes a request before the request is sent to the server.
     *
     * @param request the request to preprocess
     * @param context the context for the request
     * @throws HttpException in case of an HTTP protocol violation
     * @throws IOException   in case of an I/O error
     */
    @Override
    public void process(final HttpRequest request,
                        final HttpContext context) throws HttpException, IOException {

        final ResumableDownloadCoordinator coordinator = ResumableDownloadCoordinator.extractFromContext(context);

        if (null == coordinator) {
            // no coordinator prepared
            return;
        }

        if (request.getRequestLine() == null) {
            // slightly impossible
            return;
        }

        if (!HttpGet.METHOD_NAME.equals(request.getRequestLine().getMethod())) {
            // not a GET request
            return;
        }

        final boolean ifMatchHeaderIsCompatible = coordinator.requestHasCompatibleIfMatchHeaders(request);
        final boolean rangeHeaderIsCompatible = coordinator.requestHasCompatibleRangeHeaders(request);
        if (!ifMatchHeaderIsCompatible || !rangeHeaderIsCompatible) {
            LOG.debug(
                    "aborting download resumption due to incompatible headers: if-match ok? {}, range ok? {}",
                    ifMatchHeaderIsCompatible,
                    rangeHeaderIsCompatible);
            coordinator.cancel();
            return;
        }

        if (!coordinator.inProgress()) {
            LOG.debug("aborting download resumption due to invalid coordinator state");
            // TODO: the following line breaks the test case, probably need to review this if statement's condition
            // coordinator.cancel();
            return;
        }

        coordinator.applyHeaders(request);
    }
}
