package com.joyent.manta.http;

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.commons.lang3.Validate.notNull;

/**
 * Request interceptor which aids in carrying out resumable downloads with the request context's
 * {@link ResumableDownloadCoordinator}. Instances are immutable
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
        notNull(request, "Request must not be null");
        notNull(context, "Context must not be null");
        notNull(request.getRequestLine(), "Request line must not be null");

        if (!HttpGet.METHOD_NAME.equalsIgnoreCase(request.getRequestLine().getMethod())) {
            // not a GET request
            return;
        }

        final ResumableDownloadCoordinator coordinator = ResumableDownloadCoordinator.extractFromContext(context);

        if (null == coordinator) {
            // no coordinator prepared by the user
            return;
        }

        coordinator.validateExistingRequestHeaders(request);

        if (coordinator.inProgress()) {
            coordinator.applyHeaders(request);
        }
    }
}
