
# Requirements

In order to run the full test suite users will need access to a running Manta endpoint. Some tests rely on additional configuration which must occur outside of this project and will be skipped if it is determined that the test cannot be run. Keep an eye on the count of skipped tests and review the relevant sections in this document if you find that any tests are skipped.

## Client Configuration

The following properties should be set using either environment variables or system properties:

 - `manta.url` or `MANTA_URL`
 - `manta.user` or `MANTA_USER`
 - `manta.key_id` or `MANTA_KEY_ID`
 - `manta.key_path` or `MANTA_KEY_PATH`
 - Optionally: `manta.it.path` or `MANTA_IT_PATH` base path for integration tests. See the section in this document titled **Integration Test Path**

## Multipart Upload (Server-side support)
Tests specific to server-side MPU functionality will be automatically skipped if the server does not support that capability.

## Roles
Tests which rely on Triton roles will require the creation of roles through the Triton customer portal. The names of the roles used in the tests will be retrieved in the same fashion as standard configuration, first checking environment variables followed by system properties and finally expecting defaults to be defined. These tests will be skipped if the client determines that the required roles do not exist.

| Default     | System Property           | Environment Variable      |
|------------ | ------------------------- | --------------------------|
| primary     | manta.it.role_primary     | MANTA_IT_ROLE_PRIMARY     |
| secondary   | manta.it.role_secondary   | MANTA_IT_ROLE_SECONDARY   |

# Integration Test Path
Integration tests only touch paths under the integration test root path which is configurable using the `manta.it.path` system property or the `MANTA_IT_PATH` environment variable. The default behavior is to compute the base path based on the user's private folder, e.g. `/my.account/stor/java-manta-integration-tests`.

# Running Tests

At the root of the project run the following Maven command in order to run all tests:

```
mvn verify
```

While the Java Cryptography Extensions are expected to be installed, it is possible to
run a subset of the test suite by adding `-DexcludedGroups=unlimited-crypto`, e.g.:
```
mvn test -DexcludedGroups=unlimited-crypto
```

# Client-side Encryption Error Coverage Table

The following table details the failure modes of the Client-side Encryption feature and the relevant test cases which address those failure modes.

| Scenario   | Write operation   | Failure mode       | Relevant test cases                                                                                                        |
|----------- | ----------------- | ------------------ | ---------------------------------------------------------------------------------------------------------------------------|
| 1          | PUT               | Network error      | com.joyent.manta.client.crypto.EncryptingEntityTest#canBeWrittenIdempotently                                               |
| 2          | PUT               | Invalid response   | com.joyent.manta.client.multipart.ServerSideMultipartManagerTest#canUploadPartValidatesResponseCode                        |
| 3          | PUT               | Server MD5 invalid |                                                                                                                            |
| 4          | MPU any part      | Network error      | com.joyent.manta.client.multipart.EncryptedServerSideMultipartManagerIT#canRetryUploadPart                                 |
| 5          | MPU any part      | Invalid response   | com.joyent.manta.client.multipart.ServerSideMultipartManagerTest#canUploadPartValidatesResponseCode                        |
| 6          | MPU last part     | Invalid response   | com.joyent.manta.client.multipart.EncryptionStateRecorderTest#testRecordAndRewindMultipleParts                             |
| 7          | MPU commit        | Network error      |                                                                                                                            |
| 8          | MPU commit        | Invalid response   | com.joyent.manta.client.multipart.EncryptedServerSideMultipartManagerIT#canRetryCompleteInCaseOfErrorDuringFinalPartUpload |
