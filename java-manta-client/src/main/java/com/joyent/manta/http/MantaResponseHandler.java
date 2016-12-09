package com.joyent.manta.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.joyent.manta.client.MantaUtils;
import com.joyent.manta.domain.ErrorDetail;
import com.joyent.manta.exception.MantaAuthenticationException;
import com.joyent.manta.exception.MantaIOException;
import com.joyent.manta.exception.MantaRemoteServerException;
import com.joyent.manta.exception.MantaResponseException;
import org.apache.commons.codec.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

/**
 * {@link ResponseHandler} implementation that handles the general case of all
 * responses from the Manta.
 *
 * @param <T> Type to handle. Handling depends on value of {@link DeserializationMode}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaResponseHandler<T> implements ResponseHandler<T> {
    /**
     * Logger instance.
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Plain text sting containing the operation name - used for logging and errors.
     */
    private final String operationName;

    /**
     * Jackson {@link ObjectMapper} implementation used for deserialization.
     */
    private final ObjectMapper mapper;

    /**
     * The target deserialization type if we are acting in ENTITY mode.
     */
    private final TypeReference<T> deserializationType;

    /**
     * An array of the expected status codes that indicate a non error state.
     */
    private final int[] expectedStatusCodes;

    /**
     * When true we treat 404 response codes as null and not an error.
     */
    private final boolean fourOhFoursAsNull;

    /**
     * Enum representing how we handle deserialization.
     */
    private final DeserializationMode deserializationMode;


    /**
     * Creates new instance configured to expect a single status code.
     *
     * @param operationName plain text sting containing the operation name - used for logging and errors
     * @param mapper jackson {@link ObjectMapper} implementation used for deserialization
     * @param deserializationType the target deserialization type if we are acting in ENTITY mode
     * @param expectedStatusCode the expected status code that indicate a non error state
     * @param fourOhFoursAsNull when true we treat 404 response codes as null and not an error
     */
    public MantaResponseHandler(final String operationName,
                                final ObjectMapper mapper,
                                final TypeReference<T> deserializationType,
                                final int expectedStatusCode,
                                final boolean fourOhFoursAsNull) {
        this(operationName, mapper, deserializationType,
                new int[] {expectedStatusCode}, fourOhFoursAsNull);
    }

    /**
     * Creates new instance configured to expect a multiple status codes.
     *
     * @param operationName plain text sting containing the operation name - used for logging and errors
     * @param mapper jackson {@link ObjectMapper} implementation used for deserialization
     * @param deserializationType the target deserialization type if we are acting in ENTITY mode
     * @param expectedStatusCodes an array of the expected status codes that indicate a non error state
     * @param fourOhFoursAsNull when true we treat 404 response codes as null and not an error
     */
    public MantaResponseHandler(final String operationName,
                                final ObjectMapper mapper,
                                final TypeReference<T> deserializationType,
                                final int[] expectedStatusCodes,
                                final boolean fourOhFoursAsNull) {
        Objects.requireNonNull(operationName, "Operation name must be present");
        Objects.requireNonNull(mapper, "Jackson object mapper must be present");
        Objects.requireNonNull(deserializationType, "Deserialization type must be present");

        this.operationName = operationName;
        this.mapper = mapper;
        this.deserializationType = deserializationType;
        this.expectedStatusCodes = expectedStatusCodes;
        // Sort codes so that we can do a binary search on them
        Arrays.sort(this.expectedStatusCodes);
        this.fourOhFoursAsNull = fourOhFoursAsNull;
        this.deserializationMode = DeserializationMode.valueForClassName(deserializationType.getType().toString());
    }

    /**
     * Processes an {@link HttpResponse} and returns some value
     * corresponding to that response.
     *
     * @param response The response to process
     * @return A value determined by the response
     * @throws ClientProtocolException in case of an http protocol error
     * @throws IOException             in case of a problem or the connection was aborted
     */
    @Override
    public T handleResponse(final HttpResponse response) throws IOException {
        final StatusLine statusLine = response.getStatusLine();
        final int statusCode = statusLine.getStatusCode();

        if (statusCode == HttpStatus.SC_UNAUTHORIZED) {
            throw new MantaAuthenticationException();
        }

        final boolean expectedStatusCodeMatched = statusCodeExpected(statusCode);

        // If everything worked and we are in VOID mode, just return null
        if (expectedStatusCodeMatched && deserializationMode.equals(DeserializationMode.VOID)) {
            return null;
        }

        // If everything worked and we are in HEADER_MAP mode, just return the response headers
        if (expectedStatusCodeMatched && deserializationMode.equals(DeserializationMode.HEADER_MAP)) {
            @SuppressWarnings("unchecked")
            T returnValue = (T)headersAsMap(response.getAllHeaders());

            return returnValue;
        }

        // If 404s are set to resolve as null return values, go ahead and return null
        if (fourOhFoursAsNull && statusCode == HttpStatus.SC_NOT_FOUND) {
            return null;
        }

        // If there is no body to parse, this is unexpected if Void isn't the return type
        if (response.getEntity() == null) {
            if (statusCode == HttpStatus.SC_INTERNAL_SERVER_ERROR) {
                throw new MantaRemoteServerException(response);
            }

            String msg = String.format("No response entity returned. Expecting a body of the"
                    + " type [%s]", deserializationType.getType());
            MantaIOException exception = new MantaIOException(msg);
            exception.setContextValue("requestId", extractRequestId(response));
            exception.setContextValue("operationName", operationName);
            exception.setContextValue("deserializationMode", deserializationMode);
            exception.setContextValue("responseHeaders", MantaUtils.asString(response.getAllHeaders()));

            throw exception;
        }

        try (InputStream in = response.getEntity().getContent()) {
            // If everything worked as expected, go ahead and deserialize JSON body
            if (expectedStatusCodeMatched) {
                final T result;

                try {
                    result = mapper.readValue(in, deserializationType);
                } catch (IOException e) {
                    final String msg = "Error deserializing entity";
                    final MantaIOException exception = new MantaIOException(msg, e);
                    exception.setContextValue("requestId", extractRequestId(response));
                    exception.setContextValue("entity", reflectionToString(response.getEntity(),
                            ToStringStyle.SHORT_PREFIX_STYLE));
                    exception.setContextValue("operationName", operationName);
                    exception.setContextValue("deserializationType", deserializationType);
                    exception.setContextValue("deserializationMode", deserializationMode);
                    exception.setContextValue("responseHeaders", MantaUtils.asString(response.getAllHeaders()));

                    throw exception;
                }

                if (result instanceof Collection) {
                    @SuppressWarnings("unchecked")
                    final Collection<Object> collection = (Collection<Object>)result;
                    @SuppressWarnings("unchecked")
                    final T wrapped = (T) new HttpCollectionResponse<>(collection, response);
                    return wrapped;
                }

                return result;
            }

            throw buildResponseException(in, statusLine, response);
        }
    }

    /**
     * Builds a new response exception.
     *
     * @param responseContentStream entity {@link InputStream} with error details
     * @param statusLine HTTP response status object
     * @param response HTTP response object
     * @return a new exception instance
     */
    protected MantaResponseException buildResponseException(
            final InputStream responseContentStream, final StatusLine statusLine,
            final HttpResponse response) {
        // Error case because we didn't get the expected status code
        ErrorDetail detail;
        String msg;
        String entityText = null;

        // Handle cases where we have an error but it isn't in JSON
        try {
            detail = mapper.readValue(responseContentStream, ErrorDetail.class);
            msg = null;
        } catch (IOException e) {
            detail = null;
            msg = "Unexpected response body - unable to parse as JSON:\n" + e.getMessage();

            if (response.getEntity().isRepeatable()) {
                final HttpEntity entity = response.getEntity();
                final int maxDebugEntitySize = 2048;

                try (InputStream ein = entity.getContent();
                     InputStream bounded = new BoundedInputStream(ein, maxDebugEntitySize)) {
                    entityText = IOUtils.toString(bounded, Charsets.UTF_8);
                } catch (IOException ioe) {
                    logger.error("Error parsing entity text", ioe);
                }
            }

            logger.warn("Problem parsing error response", e);
        }

        final MantaResponseException exception = new MantaResponseException(msg, operationName, statusLine,
                detail, extractRequestId(response));

        if (entityText != null) {
            exception.setContextValue("entityText", entityText);
        }

        exception.setContextValue("operationName", operationName);
        exception.setContextValue("deserializationMode", deserializationMode);
        exception.setContextValue("responseHeaders", MantaUtils.asString(response.getAllHeaders()));

        return exception;
    }

    /**
     * Utility method used to extract a request id from the response header.
     * @param response response header to parse for request id
     * @return string containing the request id
     */
    protected static String extractRequestId(final HttpResponse response) {
        Objects.requireNonNull(response, "Response object must be present");

        final Header requestIdHeader = response.getFirstHeader(MantaHttpHeaders.REQUEST_ID);
        final String requestId;

        if (requestIdHeader == null) {
            requestId = null;
        } else {
            requestId = requestIdHeader.getValue();
        }

        return requestId;
    }

    /**
     * Converts an array of {@link Header} objects to a {@link Map} indexed by the header
     * names for easy lookup.
     * @param headers non-null array of headers
     * @return header array as Map
     */
    protected Map<String, Header> headersAsMap(final Header[] headers) {
        Objects.requireNonNull(headers, "Headers must be present");

        if (headers.length == 0) {
            return Collections.emptyMap();
        }
        final Map<String, Header> headerMap = new LinkedHashMap<>(headers.length);

        for (final Header h : headers) {
            if (h.getName() == null) {
                continue;
            }

            headerMap.put(h.getName(), h);
        }

        return Collections.unmodifiableMap(headerMap);
    }

    /**
     * Checks to see if a given status code is one of the expected codes
     * for this response.
     *
     * @param statusCode status code as integer to check against expectations
     * @return true if expected, otherwise false
     */
    protected boolean statusCodeExpected(final int statusCode) {
        return Arrays.binarySearch(expectedStatusCodes, statusCode) >= 0;
    }
}
