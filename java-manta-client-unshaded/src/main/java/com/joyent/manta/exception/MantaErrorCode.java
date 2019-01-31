/*
 * Copyright (c) 2015-2019, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Enum representing all of the known error codes in Manta.
 * The design for using statically defined error codes (rest codes) is due to limitations
 * in Java's matching syntax. In order to provide for clean logic branches for handling
 * various server-side conditions, it works best in Java to use an enum. This allows
 * us to easily use switch / case statements.
 * However, it has the limitation where all of the known cases need to be predefined
 *
 * @see <a href="https://apidocs.joyent.com/manta/api.html#errors">Manta Errors</a>
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@SuppressWarnings("checkstyle:JavadocVariable")
public enum MantaErrorCode {
    ACCOUNT_BLOCKED_ERROR("AccountBlocked"),
    ACCOUNT_DOES_NOT_EXIST_ERROR("AccountDoesNotExist"),
    AUTH_SCHEME_ERROR("AuthScheme"),
    AUTHORIZATION_ERROR("Authorization"),
    AUTHORIZATION_FAILED_ERROR("AuthorizationFailed"),
    AUTHORIZATION_REQUIRED_ERROR("AuthorizationRequired"),
    AUTHORIZATION_SCHEME_NOT_ALLOWED_ERROR("AuthorizationSchemeNotAllowed"),
    BAD_REQUEST_ERROR("BadRequest"),
    CHECKSUM_ERROR("Checksum"),
    CONCURRENT_REQUEST_ERROR("ConcurrentRequest"),
    CONTENT_LENGTH_ERROR("ContentLength"),
    CONTENT_LENGTH_REQUIRED_ERROR("ContentLengthRequired"),
    CONTENT_MD5_MISMATCH_ERROR("ContentMD5Mismatch"),
    DIRECTORY_DOES_NOT_EXIST_ERROR("DirectoryDoesNotExist"),
    DIRECTORY_EXISTS_ERROR("DirectoryExists"),
    DIRECTORY_LIMIT_EXCEEDED_ERROR("DirectoryLimitExceeded"),
    DIRECTORY_NOT_EMPTY_ERROR("DirectoryNotEmpty"),
    DIRECTORY_OPERATION_ERROR("DirectoryOperation"),
    ENTITY_EXISTS_ERROR("EntityExists"),
    /**
     * Known component sources: muskie
     */
    ENTITY_EXISTS_MUSKIE_ERROR("EntityAlreadyExists"),
    /**
     * Known component sources: muskie
     */
    ETAG_CONFLICT_ERROR("EtagConflictError"),
    EXPECTED_UPGRADE_ERROR("ExpectedUpgrade"),
    INTERNAL_ERROR("InternalError"),
    /**
     * Known component sources: muskie
     */
    INVALID_ALGORITHM_ERROR("InvalidAlgorithm"),
    INVALID_ARGUMENT_ERROR("InvalidArgumentError"),
    INVALID_AUTH_TOKEN_ERROR("InvalidAuthenticationToken"),
    INVALID_CREDENTIALS_ERROR("InvalidCredentials"),
    INVALID_DURABILITY_LEVEL_ERROR("InvalidDurabilityLevel"),
    INVALID_HEADER_ERROR("InvalidHeader"),
    INVALID_HTTP_AUTHENTICATION_TOKEN_ERROR("InvalidHttpAuthenticationToken"),
    INVALID_JOB_ERROR("InvalidJob"),
    INVALID_JOB_STATE_ERROR("InvalidJobState"),
    INVALID_KEY_ID_ERROR("InvalidKeyId"),
    INVALID_LINK_ERROR("InvalidLink"),
    INVALID_LIMIT_ERROR("InvalidLimit"),
    INVALID_MAX_CONTENT_LENGTH_ERROR("InvalidMaxContentLength"),
    INVALID_PARAMETER_ERROR("InvalidParameter"),
    /**
     * Known component sources: muskie
     */
    INVALID_PATH_ERROR("InvalidResource"),
    INVALID_QUERY_STRING_AUTHENTICATION_ERROR("InvalidQueryStringAuthentication"),
    INVALID_RESOURCE_ERROR("InvalidResource"),
    INVALID_ROLE_ERROR("InvalidRole"),
    INVALID_ROLE_TAG_ERROR("InvalidRoleTag"),
    INVALID_SIGNATURE_ERROR("InvalidSignature"),
    INVALID_UPDATE_ERROR("InvalidUpdate"),
    JOB_NOT_FOUND_ERROR("JobNotFound"),
    JOB_STATE_ERROR("JobState"),
    /**
     * Known component sources: muskie
     */
    JOB_STATE_MUSKIE_ERROR("InvalidJobState"),
    KEY_DOES_NOT_EXIST_ERROR("KeyDoesNotExist"),
    LINK_NOT_FOUND_ERROR("LinkNotFound"),
    LINK_NOT_OBJECT_ERROR("LinkNotObject"),
    LINK_REQUIRED_ERROR("LinkRequired"),
    /**
     * Known component sources: muskie
     */
    LINK_REQUIRED_MUSKIE_ERROR("LocationRequired"),
    LOCATION_REQUIRED_ERROR("LocationRequired"),
    /**
     * Known component sources: muskie
     */
    MAX_CONTENT_LENGTH_ERROR("InvalidMaxContentLength"),
    MAX_CONTENT_LENGTH_EXCEEDED_ERROR("MaxContentLengthExceeded"),
    /**
     * Known component sources: muskie
     */
    MAX_SIZE_EXCEEDED_ERROR("MaxContentLengthExceeded"),
    METHOD_NOT_ALLOWED("MethodNotAllowedError"),
    MISSING_PERMISSION_ERROR("MissingPermission"),
    /**
     * Known component sources: muskie
     */
    MULTIPART_UPLOAD_CREATE_ERROR("MultipartUploadInvalidArgument"),
    MULTIPART_UPLOAD_INVALID_ARGUMENT("MultipartUploadInvalidArgument"),
    /**
     * Known component sources: muskie
     */
    MULTIPART_UPLOAD_INVALID_ARGUMENT_ERROR("MultipartUploadInvalidArgument"),
    /**
     * Known component sources: muskie
     */
    MULTIPART_UPLOAD_PART_SIZE("MultipartUploadPartSize"),
    /**
     * Known component sources: muskie
     */
    MULTIPART_UPLOAD_STATE_ERROR("InvalidMultipartUploadState"),
    NO_API_SERVERS_AVAILABLE("NoApiServersAvailable"),
    NO_MATCHING_ROLE_TAG_ERROR("NoMatchingRoleTag"),
    NOT_ACCEPTABLE_ERROR("NotAcceptable"),
    NOT_ENOUGH_SPACE_ERROR("NotEnoughSpace"),
    /**
     * Known component sources: muskie
     */
    NOT_IMPLEMENTED_ERROR("NotImplemented"),
    /**
     * Known component sources: muskie
     */
    OBJECT_NOT_FOUND_ERROR("ObjectNotFoundError"),
    OPERATION_NOT_ALLOWED_ON_DIRECTORY_ERROR("OperationNotAllowedOnDirectory"),
    OPERATION_NOT_ALLOWED_ON_ROOT_DIRECTORY_ERROR("OperationNotAllowedOnRootDirectory"),
    PARENT_NOT_DIRECTORY_ERROR("ParentNotDirectory"),
    PRECONDITION_FAILED_ERROR("PreconditionFailed"),
    /**
     * Known component sources: muskie
     */
    PRECONDITION_FAILED_MUSKIE_ERROR("PreconditionFailedError"),
    PRE_SIGNED_REQUEST_ERROR("PreSignedRequest"),
    /**
     * Known component sources: muskie
     */
    PRE_SIGNED_REQUEST_MUSKIE_ERROR("InvalidQueryStringAuthentication"),
    REQUEST_ENTITY_TOO_LARGE_ERROR("RequestEntityTooLarge"),
    /**
     * Known component sources: muskie
     */
    REQUEST_TIMEOUT_ERROR("RequestTimeout"),
    REQUESTED_RANGE_NOT_SATISFIABLE_ERROR("RequestedRangeNotSatisfiable"),
    RESOURCE_NOT_FOUND_ERROR("ResourceNotFound"),
    ROOT_DIRECTORY_ERROR("RootDirectory"),
    /**
     * Known component sources: muskie
     */
    ROOT_DIRECTORY_MUSKIE_ERROR("OperationNotAllowedOnRootDirectory"),
    SECURE_TRANSPORT_REQUIRED_ERROR("SecureTransportRequired"),
    SERVICE_UNAVAILABLE_ERROR("ServiceUnavailable"),
    SHARDS_EXHAUSTED_ERROR("InternalError"),
    SNAPLINKS_DISABLED_ERROR("SnaplinksDisabledError"),
    SOURCE_OBJECT_NOT_FOUND_ERROR("SourceObjectNotFound"),
    SSL_REQUIRED_ERROR("SSLRequired"),
    /**
     * Known component sources: muskie
     */
    SSL_REQUIRED_MUSKIE_ERROR("SecureTransportRequired"),
    /**
     * Known component sources: muskie
     */
    THROTTLED_ERROR("ThrottledError"),
    /**
     * Known component sources: muskie
     */

    UNIQUE_ATTRIBUTE_ERROR("UniqueAttributeError"),
    UPLOAD_ABANDONED_ERROR("UploadAbandoned"),
    UPLOAD_TIMEOUT_ERROR("UploadTimeout"),
    USER_DOES_NOT_EXIST_ERROR("UserDoesNotExist"),
    USER_TASK_ERROR("UserTaskError"),

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
     * Thread-safe reference to a LOOKUP map for codes to enum. This is populated
     * upon first invocation of valueOfCode().
     */
    private static final AtomicReference<Map<String, MantaErrorCode>> LOOKUP =
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
        if (LOOKUP.get() == null) {
            Map<String, MantaErrorCode> backing = new HashMap<>(values().length);
            for (MantaErrorCode m : values()) {
                backing.put(m.getCode(), m);
            }
            LOOKUP.compareAndSet(null, Collections.unmodifiableMap(backing));
        }

        MantaErrorCode found = LOOKUP.get().getOrDefault(serverCode, UNKNOWN_ERROR);

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
