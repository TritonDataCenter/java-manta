/*
 * Copyright (c) 2019, Joyent, Inc. All rights reserved.
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
 * @see <a href="https://github.com/joyent/node-mahi/blob/master/lib/errors.js">Mahi Errors</a>
 * @see <a href="https://github.com/joyent/manta-marlin/blob/master/common/lib/errors.js">Marlin Errors</a>
 * @see <a href="https://github.com/joyent/manta-muskie/blob/master/lib/errors.js">Muskie Errors</a>
 * @see <a href="https://github.com/joyent/piranha-storage/blob/master/lib/errors.js">Piranha-Storage Errors</a>
 * @see <a href="https://joyent.github.io/manta-debugging-guide/#_incident_response_decision_tree">Manta Debugging Guide</a>
 * @see <a href="https://joyent.github.io/manta-debugging-guide/#_quick_references">Quick References</a>
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @author <a href="https://github.com/nairashwin952013">Ashwin A Nair</a>
 */
@SuppressWarnings("checkstyle:JavadocVariable")
public enum MantaErrorCode {
    /**
     * Known component sources: muskie, piranha.
     */
    ACCOUNT_BLOCKED_ERROR("AccountBlocked"),
    /**
     * Known component sources: muskie, piranha, mahi, sdc-imgapi.
     * No account exists with the given login
     */
    ACCOUNT_DOES_NOT_EXIST_ERROR("AccountDoesNotExist"),
    AUTH_SCHEME_ERROR("AuthScheme"),
    /**
     * Known component sources: muskie.
     */
    AUTH_SCHEME_MUSKIE_ERROR("AuthorizationSchemeNotAllowed"),
    /**
     * Known component sources: marlin.
     */
    AUTHORIZATION_ERROR("Authorization"),
    /**
     * Known component sources: muskie.
     */
    AUTHORIZATION_MUSKIE_ERROR("AuthorizationFailed"),
    /**
     * Known component sources: muskie, piranha.
     */
    AUTHORIZATION_FAILED_ERROR("AuthorizationFailed"),
    /**
     * Known component sources: muskie, piranha.
     */
    AUTHORIZATION_REQUIRED_ERROR("AuthorizationRequired"),
    /**
     * Known component sources: muskie.
     */
    AUTHORIZATION_SCHEME_NOT_ALLOWED_ERROR("AuthorizationSchemeNotAllowed"),
    /**
     * Known component sources: muppet, mahi, haproxy, sdc-cloudapi.
     */
    BAD_REQUEST_ERROR("BadRequest"),
    CHECKSUM_ERROR("Checksum"),
    /**
     * Known component sources: muskie, piranha.
     */
    CHECKSUM_MUSKIE_ERROR("ContentMD5Mismatch"),
    /**
     * Known component sources: muskie.
     */
    CONCURRENT_REQUEST_ERROR("ConcurrentRequest"),
    CONTENT_LENGTH_ERROR("ContentLength"),
    /**
     * Known component sources: muskie.
     */
    CONTENT_LENGTH_MUSKIE_ERROR("ContentLengthRequired"),
    /**
     * Known component sources: muskie, piranha.
     */
    CONTENT_LENGTH_REQUIRED_ERROR("ContentLengthRequired"),
    /**
     * Known component sources: muskie.
     */
    CONTENT_MD5_MISMATCH_ERROR("ContentMD5Mismatch"),
    /**
     * Known component sources: mahi.
     */
    CROSS_ACCOUNT_ERROR("CrossAccount"),
    /**
     * Known component sources: muskie, piranha.
     */
    DIRECTORY_DOES_NOT_EXIST_ERROR("DirectoryDoesNotExist"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    DIRECTORY_EXISTS_ERROR("DirectoryExists"),
    /**
     * Known component sources: muskie, piranha.
     */
    DIRECTORY_LIMIT_ERROR("DirectoryLimitExceeded"),
    /**
     * Known component sources: muskie.
     */
    DIRECTORY_LIMIT_EXCEEDED_ERROR("DirectoryLimitExceeded"),
    /**
     * Known component sources: muskie, piranha.
     */
    DIRECTORY_NOT_EMPTY_ERROR("DirectoryNotEmpty"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    DIRECTORY_OPERATION_ERROR("DirectoryOperation"),
    /**
     * Known component sources: muskie, piranha.
     */
    DIRECTORY_OPERATION_MUSKIE_ERROR("OperationNotAllowedOnDirectory"),
    /**
     * If the specified path already exists and is not a directory
     */
    ENTITY_EXISTS_ERROR("EntityExists"),
    /**
     * Known component sources: muskie.
     */
    ENTITY_EXISTS_MUSKIE_ERROR("EntityAlreadyExists"),
    /**
     * Known component sources: muskie, moray, electric-moray.
     */
    ETAG_CONFLICT_ERROR("EtagConflictError"),
    /**
     * Known component sources: muskie.
     */
    EXPECTED_UPGRADE_ERROR("ExpectedUpgrade"),
    /**
     * Known component sources: muskie, moray, piranha, marlin.
     * This generally indicates a server-side bug.
     */
    INTERNAL_ERROR("InternalError"),
    /**
     * Known component sources: muskie.
     */
    INVALID_ALGORITHM_ERROR("InvalidAlgorithm"),
    /**
     * Known component sources: muskie, marlin.
     */
    INVALID_ARGUMENT_ERROR("InvalidArgumentError"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_AUTH_TOKEN_ERROR("InvalidAuthenticationToken"),
    /**
     * Known component sources: piranha.
     */
    INVALID_CREDENTIALS_ERROR("InvalidCredentials"),
    /**
     * Known component sources: muskie.
     */
    INVALID_DURABILITY_LEVEL_ERROR("InvalidDurabilityLevel"),
    /**
     *  Known component sources: sdc-imgapi, sdc-amon.
     *  An invalid header was given in the request.
     */
    INVALID_HEADER_ERROR("InvalidHeader"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_HTTP_AUTH_TOKEN_ERROR("InvalidHttpAuthenticationToken"),
    /**
     * Known component sources: muskie.
     */
    INVALID_HTTP_AInvalidHttpAuthenticationTokeUTHENTICATION_TOKEN_ERROR("InvalidHttpAuthenticationToken"),
    /**
     * Known component sources: muskie.
     */
    INVALID_JOB_ERROR("InvalidJob"),
    /**
     * Known component sources: muskie.
     */
    INVALID_JOB_STATE_ERROR("InvalidJobState"),
    /**
     * Known component sources: piranha.
     */
    INVALID_KEY_ERROR("InvalidKey"),
    /**
     * Known component sources: muskie.
     */
    INVALID_KEY_ID_ERROR("InvalidKeyId"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_LINK_ERROR("InvalidLink"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    INVALID_LIMIT_ERROR("InvalidLimit"),
    /**
     * Known component sources: muskie.
     */
    INVALID_LIMIT_MUSKIE_ERROR("InvalidArgumentError"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_MAX_CONTENT_LENGTH_ERROR("InvalidMaxContentLength"),
    /**
     * Known component sources: muskie, sdc-amon, kbmapi, sdc-cloudapi, sdc-imgapi, sdc-napi.
     */
    INVALID_PARAMETER_ERROR("InvalidParameter"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_PATH_ERROR("InvalidResource"),
    /**
     * Known component sources: muskie.
     */
    INVALID_QUERY_STRING_AUTHENTICATION_ERROR("InvalidQueryStringAuthentication"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_RESOURCE_ERROR("InvalidResource"),
    /**
     * Known component sources: muskie, piranha, mahi.
     */
    INVALID_ROLE_ERROR("InvalidRole"),
    /**
     * Known component sources: muskie, piranha.
     */
    INVALID_ROLE_TAG_ERROR("InvalidRoleTag"),
    /**
     * Known component sources: muskie, piranha, mahi.
     */
    INVALID_SIGNATURE_ERROR("InvalidSignature"),
    /**
     * Known component sources: muskie.
     */
    INVALID_UPDATE_ERROR("InvalidUpdate"),
    /**
     * Known component sources: marlin.
     */
    JOB_CANCELLED_ERROR("JobCancelledError"),
    /**
     * Known component sources: muskie.
     */
    JOB_NOT_FOUND_ERROR("JobNotFound"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    JOB_STATE_ERROR("JobState"),
    /**
     * Known component sources: muskie.
     */
    JOB_STATE_MUSKIE_ERROR("InvalidJobState"),
    /**
     * Known component sources: muskie, piranha, mahi.
     */
    KEY_DOES_NOT_EXIST_ERROR("KeyDoesNotExist"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    LINK_NOT_FOUND_ERROR("LinkNotFound"),
    /**
     * Known component sources: muskie, piranha.
     */
    LINK_NOT_FOUND_MUSKIE_ERROR("SourceObjectNotFound"),
    /**
     * Known component sources: muskie, piranha.
     */
    LINK_NOT_OBJECT_ERROR("LinkNotObject"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    LINK_REQUIRED_ERROR("LinkRequired"),
    /**
     * Known component sources: muskie, piranha.
     */
    LINK_REQUIRED_MUSKIE_ERROR("LocationRequired"),
    /**
     * Known component sources: muskie.
     */
    LOCATION_REQUIRED_ERROR("LocationRequired"),
    /**
     * Known component sources: muskie, piranha.
     */
    MAX_CONTENT_LENGTH_ERROR("InvalidMaxContentLength"),
    /**
     * Known component sources: muskie.
     */
    MAX_CONTENT_LENGTH_EXCEEDED_ERROR("MaxContentLengthExceeded"),
    /**
     * Known component sources: muskie, piranha.
     */
    MAX_SIZE_EXCEEDED_ERROR("MaxContentLengthExceeded"),
    /**
     * Known component sources: muskie.
     */
    METHOD_NOT_ALLOWED("MethodNotAllowedError"),
    /**
     * Known component sources: muskie, piranha.
     */
    MISSING_PERMISSION_ERROR("MissingPermission"),
    /**
     * Known component sources: muskie.
     */
    MULTIPART_UPLOAD_CREATE_ERROR("MultipartUploadInvalidArgument"),
    /**
     * Known component sources: muskie.
     */
    MULTIPART_UPLOAD_INVALID_ARGUMENT("MultipartUploadInvalidArgument"),
    /**
     * Known component sources: muskie.
     */
    MULTIPART_UPLOAD_INVALID_ARGUMENT_ERROR("MultipartUploadInvalidArgument"),
    /**
     * Known component sources: muskie.
     */
    MULTIPART_UPLOAD_PART_SIZE("MultipartUploadPartSize"),
    /**
     * Known component sources: muskie.
     */
    MULTIPART_UPLOAD_STATE_ERROR("InvalidMultipartUploadState"),
    /**
     * Known component sources: muppet.
     */
    NO_API_SERVERS_AVAILABLE("NoApiServersAvailable"),
    /**
     * Known component sources: moray, marlin.
     * Moray indicates a transient availability error due to request overload.
     */
    NO_DATABASE_PEERS_ERROR("NoDatabasePeersError"),
    /**
     * Known component sources: muskie, piranha, mahi.
     */
    NO_MATCHING_ROLE_TAG_ERROR("NoMatchingRoleTag"),
    /**
     * Known component sources: muskie.
     */
    NOT_ACCEPTABLE_ERROR("NotAcceptable"),
    /**
     * Known component sources: muskie, piranha.
     * Manta does not have enough space available on any storage nodes for the
     * write that was requested.
     */
    NOT_ENOUGH_SPACE_ERROR("NotEnoughSpace"),
    /**
     * Known component sources: muskie, piranha.
     */
    NOT_IMPLEMENTED_ERROR("NotImplemented"),
    /**
     * Known component sources: muskie, moray, electric-moray, marlin, manta-mako, manta-mola.
     */
    OBJECT_NOT_FOUND_ERROR("ObjectNotFoundError"),
    /**
     * Known component sources: muskie, piranha.
     */
    OPERATION_NOT_ALLOWED_ON_DIRECTORY_ERROR("OperationNotAllowedOnDirectory"),
    /**
     * Known component sources: muskie.
     */
    OPERATION_NOT_ALLOWED_ON_ROOT_DIRECTORY_ERROR("OperationNotAllowedOnRootDirectory"),
    /**
     * Known component sources: muskie, piranha.
     */
    PARENT_NOT_DIRECTORY_ERROR("ParentNotDirectory"),
    /**
     * Known component sources: sdc-napi.
     */
    PRECONDITION_FAILED_ERROR("PreconditionFailed"),
    /**
     * Known component sources: muskie, sdc-napi, sdc-cnapi.
     */
    PRECONDITION_FAILED_MUSKIE_ERROR("PreconditionFailedError"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    PRE_SIGNED_REQUEST_ERROR("PreSignedRequest"),
    /**
     * Known component sources: muskie.
     */
    PRE_SIGNED_REQUEST_MUSKIE_ERROR("InvalidQueryStringAuthentication"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    REQUEST_ENTITY_TOO_LARGE_ERROR("RequestEntityTooLarge"),
    /**
     * Known component sources: muskie, muppet, marlin.
     */
    REQUEST_TIMEOUT_ERROR("RequestTimeout"),
    /**
     * Known component sources: muskie.
     */
    REQUESTED_RANGE_NOT_SATISFIABLE_ERROR("RequestedRangeNotSatisfiable"),
    /**
     * Known component sources: muskie, piranha, marlin.
     */
    RESOURCE_NOT_FOUND_ERROR("ResourceNotFound"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    ROOT_DIRECTORY_ERROR("RootDirectory"),
    /**
     * Known component sources: muskie.
     */
    ROOT_DIRECTORY_MUSKIE_ERROR("OperationNotAllowedOnRootDirectory"),
    /**
     * Known component sources: mahi.
     */
    RULES_EVALUATION_FAILED_ERROR("RulesEvaluationFailed"),
    /**
     * Known component sources: muskie.
     */
    SECURE_TRANSPORT_REQUIRED_ERROR("SecureTransportRequired"),
    /**
     * Known component sources: muskie, muppet, moray, marlin, piranha. Major Cases are:
     * At least one Moray instance is at its maximum queue length and
     * is refusing new requests.
     *
     * There are not enough online storage nodes to handle the upload.
     *
     * Muskie did not respond to the request quickly enough.
     */
    SERVICE_UNAVAILABLE_ERROR("ServiceUnavailable"),
    /**
     * Known component sources: muskie, moray.
     */
    SHARDS_EXHAUSTED_ERROR("InternalError"),
    /**
     * Known component sources: muskie.
     */
    SNAPLINKS_DISABLED_ERROR("SnaplinksDisabledError"),
    /**
     * Known component sources: muskie, piranha.
     */
    SOURCE_OBJECT_NOT_FOUND_ERROR("SourceObjectNotFound"),
    /**
     * No known server-side code with that name, should be deprecated.
     */
    SSL_REQUIRED_ERROR("SSLRequired"),
    /**
     * Known component sources: muskie.
     */
    SSL_REQUIRED_MUSKIE_ERROR("SecureTransportRequired"),
    /**
     * Known component sources: marlin, muskie.
     */
    TASK_INIT_ERROR("TaskInitError"),
    /**
     * Known component sources: marlin.
     */
    TASK_KILLED_ERROR("TaskKilledError"),
    /**
     * Known component sources: muskie, moray.
     */
    THROTTLED_ERROR("ThrottledError"),
    /**
     * Known component sources: muskie, moray.
     */
    UNIQUE_ATTRIBUTE_ERROR("UniqueAttributeError"),
    /**
     * Known component sources: muskie, piranha.
     * This is recorded when a client closes its socket before finishing a request.
     * Elevated Muskie latency may be the reason.
     */
    UPLOAD_ABANDONED_ERROR("UploadAbandoned"),
    /**
     * Known component sources: muskie, piranha.
     */
    UPLOAD_TIMEOUT_ERROR("UploadTimeout"),
    /**
     * Known component sources: muskie, mahi, piranha.
     */
    USER_DOES_NOT_EXIST_ERROR("UserDoesNotExist"),
    /**
     * Known component sources: marlin, muskie.
     * User's script returned a non-zero status or one of its processes dumped core.
     */
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
