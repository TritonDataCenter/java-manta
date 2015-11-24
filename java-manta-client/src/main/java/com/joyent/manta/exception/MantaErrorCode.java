package com.joyent.manta.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Enum representing all of the known error codes in Manta.
 *
 * @see <a href="https://apidocs.joyent.com/manta/api.html#errors">Manta Errors</a>
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
@SuppressWarnings("checkstyle:JavadocVariable")
public enum MantaErrorCode {
    ACCOUNT_DOES_NOT_EXIST_ERROR("AccountDoesNotExist"),
    AUTH_SCHEME_ERROR("AuthScheme"),
    AUTHORIZATION_ERROR("Authorization"),
    BAD_REQUEST_ERROR("BadRequest"),
    CHECKSUM_ERROR("Checksum"),
    CONCURRENT_REQUEST_ERROR("ConcurrentRequest"),
    CONTENT_LENGTH_ERROR("ContentLength"),
    CONTENT_MD5_MISMATCH_ERROR("ContentMD5Mismatch"),
    ENTITY_EXISTS_ERROR("EntityExists"),
    INVALID_ARGUMENT_ERROR("InvalidArgument"),
    INVALID_AUTH_TOKEN_ERROR("InvalidAuthToken"),
    INVALID_CREDENTIALS_ERROR("InvalidCredentials"),
    INVALID_DURABILITY_LEVEL_ERROR("InvalidDurabilityLevel"),
    INVALID_KEY_ID_ERROR("InvalidKeyId"),
    INVALID_JOB_ERROR("InvalidJob"),
    INVALID_LINK_ERROR("InvalidLink"),
    INVALID_LIMIT_ERROR("InvalidLimit"),
    INVALID_ROLE_TAG_ERROR("InvalidRoleTag"),
    INVALID_SIGNATURE_ERROR("InvalidSignature"),
    INVALID_UPDATE_ERROR("InvalidUpdate"),
    DIRECTORY_DOES_NOT_EXIST_ERROR("DirectoryDoesNotExist"),
    DIRECTORY_EXISTS_ERROR("DirectoryExists"),
    DIRECTORY_NOT_EMPTY_ERROR("DirectoryNotEmpty"),
    DIRECTORY_OPERATION_ERROR("DirectoryOperation"),
    INTERNAL_ERROR("Internal"),
    JOB_NOT_FOUND_ERROR("JobNotFound"),
    JOB_STATE_ERROR("JobState"),
    KEY_DOES_NOT_EXIST_ERROR("KeyDoesNotExist"),
    NOT_ACCEPTABLE_ERROR("NotAcceptable"),
    NOT_ENOUGH_SPACE_ERROR("NotEnoughSpace"),
    LINK_NOT_FOUND_ERROR("LinkNotFound"),
    LINK_NOT_OBJECT_ERROR("LinkNotObject"),
    LINK_REQUIRED_ERROR("LinkRequired"),
    PARENT_NOT_DIRECTORY_ERROR("ParentNotDirectory"),
    PRECONDITION_FAILED_ERROR("PreconditionFailed"),
    PRE_SIGNED_REQUEST_ERROR("PreSignedRequest"),
    REQUEST_ENTITY_TOO_LARGE_ERROR("RequestEntityTooLarge"),
    RESOURCE_NOT_FOUND_ERROR("ResourceNotFound"),
    ROOT_DIRECTORY_ERROR("RootDirectory"),
    SERVICE_UNAVAILABLE_ERROR("ServiceUnavailable"),
    SSL_REQUIRED_ERROR("SSLRequired"),
    UPLOAD_TIMEOUT_ERROR("UploadTimeout"),
    USER_DOES_NOT_EXIST_ERROR("UserDoesNotExist"),
    /**
     * Error code indicating that an error not in the list of enums was returned.
     */
    UNKNOWN_ERROR("UnknownError"),
    /**
     * Error code indicating that no usable code was returned from the API.
     */
    NO_CODE_ERROR(null),
    /**
     * Error code used when we want to indicate that the code has yet to be defined.
     */
    UNDEFINED("X-Undefined");

    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(MantaErrorCode.class);

    /**
     * Error code as represented in Manta.
     */
    private final String code;


    /**
     * Thread-safe reference to a lookup map for codes to enum. This is populated
     * upon first invocation of valueOfCode().
     */
    private static AtomicReference<Map<String, MantaErrorCode>> lookup =
            new AtomicReference<>();

    /**
     * Creates a new instance of the error code.
     *
     * @param code Manta error code
     */
    MantaErrorCode(final String code) {
        this.code = code;
    }

    /**
     * @return Manta error code as defined in the API.
     */
    public String getCode() {
        return code;
    }


    /**
     * Looks up a {@link MantaErrorCode} by its code value.
     *
     * @param serverCode code value to search for. Null is acceptable
     * @return Manta error code enum associated with serverCode parameter
     */
    public static MantaErrorCode valueOfCode(final String serverCode) {
        if (lookup.get() == null) {
            Map<String, MantaErrorCode> backing = new HashMap<>(values().length);
            for (MantaErrorCode m : values()) {
                backing.put(m.getCode(), m);
            }
            lookup.compareAndSet(null, Collections.unmodifiableMap(backing));
        }

        MantaErrorCode found = lookup.get().getOrDefault(serverCode, UNKNOWN_ERROR);

        if (found.equals(UNKNOWN_ERROR)) {
            LOG.warn("Unknown error code received from Manta: {}", serverCode);
        }

        return found;
    }


    /**
     * Looks up a {@link MantaErrorCode} by its code value.
     *
     * @param object object to read .toString() from, if null - it is passed on
     * @return Manta error code enum associated with serverCode parameter
     */
    public static MantaErrorCode valueOfCode(final Object object) {
        if (object == null) {
            return valueOfCode(null);
        }

        return valueOfCode(object.toString());
    }
}
