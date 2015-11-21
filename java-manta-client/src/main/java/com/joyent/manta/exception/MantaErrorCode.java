package com.joyent.manta.exception;

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
    AUTH_SCHEME_ERROR("AuthSchemeError"),
    AUTHORIZATION_ERROR("AuthorizationError"),
    BAD_REQUEST_ERROR("BadRequestError"),
    CHECKSUM_ERROR("ChecksumError"),
    CONCURRENT_REQUEST_ERROR("ConcurrentRequestError"),
    CONTENT_LENGTH_ERROR("ContentLengthError"),
    CONTENT_MD5_MISMATCH_ERROR("ContentMD5MismatchError"),
    ENTITY_EXISTS_ERROR("EntityExistsError"),
    INVALID_ARGUMENT_ERROR("InvalidArgumentError"),
    INVALID_AUTH_TOKEN_ERROR("InvalidAuthTokenError"),
    INVALID_CREDENTIALS_ERROR("InvalidCredentialsError"),
    INVALID_DURABILITY_LEVEL_ERROR("InvalidDurabilityLevelError"),
    INVALID_KEY_ID_ERROR("InvalidKeyIdError"),
    INVALID_JOB_ERROR("InvalidJobError"),
    INVALID_LINK_ERROR("InvalidLinkError"),
    INVALID_LIMIT_ERROR("InvalidLimitError"),
    INVALID_SIGNATURE_ERROR("InvalidSignatureError"),
    INVALID_UPDATE_ERROR("InvalidUpdateError"),
    DIRECTORY_DOES_NOT_EXIST_ERROR("DirectoryDoesNotExistError"),
    DIRECTORY_EXISTS_ERROR("DirectoryExistsError"),
    DIRECTORY_NOT_EMPTY_ERROR("DirectoryNotEmptyError"),
    DIRECTORY_OPERATION_ERROR("DirectoryOperationError"),
    INTERNAL_ERROR("InternalError"),
    JOB_NOT_FOUND_ERROR("JobNotFoundError"),
    JOB_STATE_ERROR("JobStateError"),
    KEY_DOES_NOT_EXIST_ERROR("KeyDoesNotExistError"),
    NOT_ACCEPTABLE_ERROR("NotAcceptableError"),
    NOT_ENOUGH_SPACE_ERROR("NotEnoughSpaceError"),
    LINK_NOT_FOUND_ERROR("LinkNotFoundError"),
    LINK_NOT_OBJECT_ERROR("LinkNotObjectError"),
    LINK_REQUIRED_ERROR("LinkRequiredError"),
    PARENT_NOT_DIRECTORY_ERROR("ParentNotDirectoryError"),
    PRECONDITION_FAILED_ERROR("PreconditionFailedError"),
    PRE_SIGNED_REQUEST_ERROR("PreSignedRequestError"),
    REQUEST_ENTITY_TOO_LARGE_ERROR("RequestEntityTooLargeError"),
    RESOURCE_NOT_FOUND_ERROR("ResourceNotFoundError"),
    ROOT_DIRECTORY_ERROR("RootDirectoryError"),
    SERVICE_UNAVAILABLE_ERROR("ServiceUnavailableError"),
    SSL_REQUIRED_ERROR("SSLRequiredError"),
    UPLOAD_TIMEOUT_ERROR("UploadTimeoutError"),
    USER_DOES_NOT_EXIST_ERROR("UserDoesNotExistError"),
    /**
     * Error code indicating that an error not in the list of enums was returned.
     */
    UNKNOWN_ERROR("UnknownError"),
    /**
     * Error code indicating that no usable code was returned from the API.
     */
    NO_CODE_ERROR(null);


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

        return lookup.get().getOrDefault(serverCode, UNKNOWN_ERROR);
    }
}
