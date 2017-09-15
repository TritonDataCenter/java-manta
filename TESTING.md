
# Requirements

In order to run the full test suite users will need access to a running Manta
endpoint. Some tests rely on additional configuration which must occur outside
of this project and will be skipped if it is determined that the test cannot
be run. Keep an eye on the count of skipped tests and review the relevant
sections in this document if you find that any tests are skipped.

## Client Configuration

The following properties should be set using either environment variables or
system properties:

 - `manta.url` or `MANTA_URL`
 - `manta.user` or `MANTA_USER`
 - `manta.key_id` or `MANTA_KEY_ID`
 - `manta.key_path` or `MANTA_KEY_PATH`

## Multipart Upload (Server-side support)
Tests specific to server-side MPU functionality will be automatically skipped
if the server con does not support that capability.

## Roles
Tests which rely on [Triton roles](https://docs.joyent.com/public-cloud/rbac/roles)
will require the creation of roles through the Triton customer portal. The names
of the roles used in the tests will be retrieved in the same fashion as standard
configuration, first checking environment variables followed by system properties
and finally expecting defaults to be defined. These tests will be skipped if it
is determined that the roles do not exist.

| Default     | System Property           | Environment Variable      |
|------------ | ------------------------- | --------------------------|
| primary     | manta.it.role_primary     | MANTA_IT_ROLE_PRIMARY     |
| secondary   | manta.it.role_secondary   | MANTA_IT_ROLE_SECONDARY   |

## Integration Test Path
Integration tests only touch paths under the integration test root path which
is configurable using the `manta.it.path` system property or the `MANTA_IT_PATH`
environment variable. The default behavior is to compute the base path based on
the user's private folder, e.g. `/my.account/stor/java-manta-integration-tests`.

This is useful if you want to run a single test case using `-Dit.test=${TESTNAME}`
and want the data created by that test to live in a specific directory. This also
makes it possible to isolate entire runs of the test suite, e.g.

```
mvn verify -Dmanta.it.path="$MANTA_USER/stor/$(git rev-parse HEAD)"
```

# Running Tests

Run `mvn verify` from the project root to run all tests. Some Maven goals will
not work

While the Java Cryptography Extensions are expected to be installed, it is
possible torun a subset of the test suite by adding
`-DexcludedGroups=unlimited-crypto`, e.g.:
```
mvn test -DexcludedGroups=unlimited-crypto
```

# Writing Tests

The `java-manta-client` module contains unit tests. Integration tests generally
belong in the `java-manta-it` folder. In case an integration test needs to mock
or interact with classes which are shaded you may find that the test runs in
your IDE but not through `mvn verify`, failing with an error similar to the
following:

```
  required: org.apache.http.impl.client.CloseableHttpClient
  found: com.joyent.manta.org.apache.http.impl.client.CloseableHttpClient
  reason: argument mismatch; com.joyent.manta.org.apache.http.impl.client.CloseableHttpClient cannot be converted to org.apache.http.impl.client.CloseableHttpClient
```

There is still work being done to provide a workaround for this situation.

[Surefire](http://maven.apache.org/surefire/maven-surefire-plugin/) and
[Failsafe](http://maven.apache.org/surefire/maven-failsafe-plugin/) plugins are
configured to look for [TestNG](http://testng.org/doc/) configuration
in their module's `resource` folders.