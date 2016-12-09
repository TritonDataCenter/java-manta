package com.joyent.manta.exception;

import com.joyent.manta.domain.ErrorDetail;
import org.apache.http.StatusLine;

import java.io.IOException;
import java.util.Map;

/**
 * Specific {@link IOException} type that embeds the error detail information
 * returned over the REST API and allows for handling and logging to occur
 * elsewhere in a graceful fashion.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.0.0
 */
public class MantaResponseException extends MantaIOException {

    private static final long serialVersionUID = 7187748955562792221L;

    /**
     * Plain text string containing the operation name - used for logging and errors.
     */
    private final String operationName;

    /**
     * The first line of a Response message is the Status-Line, consisting
     * of the protocol version followed by a numeric status code and its
     * associated textual phrase, with each element separated by SP
     * characters.
     */
    private final StatusLine statusLine;

    /**
     * Error detail object representing the details of the REST error object.
     */
    private final ErrorDetail errorDetail;

    /**
     * Constructs an {@code MantaResponseException} with an error message
     * generated from the passed in parameters.
     *
     * @param operationName string used for logging and errors
     * @param  statusLine first line of a HTTP response message
     * @param errorDetail object representing the details of the REST error object
     * @param requestId the request id associated with the request
     */
    public MantaResponseException(final String operationName,
                                  final StatusLine statusLine,
                                  final ErrorDetail errorDetail,
                                  final String requestId) {
        this.operationName = operationName;
        this.statusLine = statusLine;
        this.errorDetail = errorDetail;
        setContextValue("requestID", requestId);
    }

    /**
     * Constructs an {@code MantaResponseException} with an error message
     * generated from the passed in parameters.
     *
     * @param message custom message to embed in generated message
     * @param operationName string used for logging and errors
     * @param  statusLine first line of a HTTP response message
     * @param errorDetail object representing the details of the REST error object
     * @param requestId the request id associated with the request
     */
    public MantaResponseException(final String message,
                                  final String operationName,
                                  final StatusLine statusLine,
                                  final ErrorDetail errorDetail,
                                  final String requestId) {
        super(message);
        this.operationName = operationName;
        this.statusLine = statusLine;
        this.errorDetail = errorDetail;
        setContextValue("requestID", requestId);
    }

    /**
     * Constructs an {@code MantaResponseException} with an error message
     * generated from the passed in parameters.
     *
     * @param message custom message to embed in generated message
     * @param cause exception to chain to this exception as the cause
     * @param operationName string used for logging and errors
     * @param  statusLine first line of a HTTP response message
     * @param errorDetail object representing the details of the REST error object
     * @param requestId the request id associated with the request
     */
    public MantaResponseException(final String message,
                                  final Throwable cause,
                                  final String operationName,
                                  final StatusLine statusLine,
                                  final ErrorDetail errorDetail,
                                  final String requestId) {
        super(message, cause);
        this.operationName = operationName;
        this.statusLine = statusLine;
        this.errorDetail = errorDetail;
        setContextValue("requestID", requestId);
    }

    /**
     * Constructs an {@code MantaResponseException} with an error message
     * generated from the passed in parameters.
     *
     * @param cause exception to chain to this exception as the cause
     * @param operationName string used for logging and errors
     * @param  statusLine first line of a HTTP response message
     * @param errorDetail object representing the details of the REST error object
     * @param requestId the request id associated with the request
     */
    public MantaResponseException(final Throwable cause,
                                  final String operationName,
                                  final StatusLine statusLine,
                                  final ErrorDetail errorDetail,
                                  final String requestId) {
        super(cause);
        this.operationName = operationName;
        this.statusLine = statusLine;
        this.errorDetail = errorDetail;
        setContextValue("requestID", requestId);
    }

    public String getOperationName() {
        return operationName;
    }

    public StatusLine getStatusLine() {
        return statusLine;
    }

    public ErrorDetail getErrorDetail() {
        return errorDetail;
    }

    @Override
    public String getMessage() {
        if (super.getMessage() == null && getErrorDetail() == null) {
            return buildErrorMessage();
        } else if (super.getMessage() == null && getErrorDetail() != null) {
            return buildErrorDetailMessage();
        } else if (super.getMessage() != null && getErrorDetail() == null) {
            return super.getMessage() + "\n" + buildErrorMessage();
        } else {
            return super.getMessage() + "\n" + buildErrorDetailMessage();
        }
    }

    @Override
    public String getLocalizedMessage() {
        return getMessage();
    }

    /**
     * Generates an error message based on the configuration of this instance when
     * the error detail property is not available.
     * @return a generated error message
     */
    protected String buildErrorMessage() {
        final String format = "CloudAPI Error on %s (HTTP %d - %s)";
        return String.format(format,
                operationName,
                statusLine.getStatusCode(),
                statusLine.getReasonPhrase());
    }

    /**
     * Generates an error message based on the configuration of this instance.
     *
     * @return a generated error message
     */
    protected String buildErrorDetailMessage() {
        final String format = "Manta error on %s (HTTP %d - %s) [%s]: %s";
        final String base = String.format(format,
                operationName,
                statusLine.getStatusCode(),
                statusLine.getReasonPhrase(),
                errorDetail.getCode(),
                errorDetail.getMessage());

        if (errorDetail.getErrors() == null || errorDetail.getErrors().isEmpty()) {
            return base;
        }

        StringBuilder b = new StringBuilder(base);

        int i = 0;

        for (Map<String, String> entry : errorDetail.getErrors()) {
            String msg = entry.get("message");
            String code = entry.get("code");
            String field = entry.get("field");

            b.append(System.lineSeparator())
             .append("    ")
             .append(i++)
             .append(": ")
             .append(msg)
             .append(" ")
             .append(String.format("[field=%s, code=%s]", field, code));
        }

        return b.toString();
    }
}
