
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

## Integration Test Path
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

The following table details the failure modes of the Client-side Encryption feature and the relevant test cases which address those failure modes. Note that MD5 validation of MPU contents is not yet supported.

| Scenario   | Write operation        | Failure mode       | Relevant test cases                                                                                                               |
| ---------- | ---------------------- | ------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| 1          | PUT                    | Network error      | [`com.joyent.manta.client.crypto.EncryptingEntityTest#canBeWrittenIdempotently`](https://github.com/tjcelaya/java-manta/blob/b738b4e2ef97c3cca230da343ca98fe975db46ac/java-manta-client/src/test/java/com/joyent/manta/client/crypto/EncryptingEntityTest.java#L231)
| 2          | PUT                    | Invalid response   | #1 and [`com.joyent.manta.http.StandardHttpHelperTest#testHttpPutValidatesResponseCodeAndThrowsWhenInvalid`](https://github.com/tjcelaya/java-manta/blob/9fd462d2c8fc10d6b4a20646667c37d339e38004/java-manta-client/src/test/java/com/joyent/manta/http/StandardHttpHelperTest.java#L97)
| 3          | PUT                    | Server MD5 invalid | #1 and [`com.joyent.manta.http.StandardHttpHelperTest#testHttpPutChecksumsCompareDifferentlyFails`](https://github.com/tjcelaya/java-manta/blob/9fd462d2c8fc10d6b4a20646667c37d339e38004/java-manta-client/src/test/java/com/joyent/manta/http/StandardHttpHelperTest.java#L152)
| 4          | MPU any part           | Network error      | [`com.joyent.manta.client.multipart.EncryptedServerSideMultipartManagerIT#canRetryUploadPart`](https://github.com/tjcelaya/java-manta/blob/5b5184ab3f95f7bbdda7a6ad840502493e85bcdd/java-manta-it/src/test/java/com/joyent/manta/client/multipart/EncryptedServerSideMultipartManagerIT.java#L503)
| 5          | MPU any part           | Invalid response   | #4 and [`com.joyent.manta.client.multipart.ServerSideMultipartManagerTest#canUploadPartValidatesResponseCode`](https://github.com/tjcelaya/java-manta/blob/a1975e77c909e1d55264170af7139dde6bc9a52a/java-manta-client/src/test/java/com/joyent/manta/client/multipart/ServerSideMultipartManagerTest.java#L139)
| 6          | MPU last part finalize | Network error      | duplicates #7
| 6          | MPU last part finalize | Invalid response   | #4 abd [`com.joyent.manta.client.multipart.EncryptionStateRecorderTest#testRecordAndRewindMultipleParts`](https://github.com/tjcelaya/java-manta/blob/263e360e3a1a2817a5d056a9793ab0c064183811/java-manta-client/src/test/java/com/joyent/manta/client/multipart/EncryptionStateRecorderTest.java#L152)
| 7          | MPU commit w/ finalize | Network error      | [`com.joyent.manta.client.multipart.EncryptedServerSideMultipartManagerIT#canRetryCompleteInCaseOfErrorDuringFinalPartUpload`](https://github.com/tjcelaya/java-manta/blob/263e360e3a1a2817a5d056a9793ab0c064183811/java-manta-it/src/test/java/com/joyent/manta/client/multipart/EncryptedServerSideMultipartManagerIT.java#L476)
| 8          | MPU commit w/ finalize | Invalid response   | #5 and #7
