package com.joyent.manta.exception;

/**
 * Enum representing all of the known error codes in Manta.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 */
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
    UNKNOWN_ERROR("UnknownError"),
    NO_CODE_ERROR(null);

    public final String code;
    private String unknownCode;

    MantaErrorCode(final String code) {
        this.code = code;
    }
}
