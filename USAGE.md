# Installation

## Requirements
* [Java 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html). Java 1.9 is not yet supported.
* [Java Cryptography Extension](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) to
  use Client-side Encryption with stronger ciphers (192/256-bit).
* [Maven 3.1.x](https://maven.apache.org/) to contribute to the project.

### CLI Requirements

Add [BouncyCastle](http://www.bouncycastle.org/latest_releases.html) as a security provider
 1. Edit "$JAVA_HOME/jre/lib/security/java.security " Add an entry for BouncyCastle
 `security.provider.11=org.bouncycastle.jce.provider.BouncyCastleProvider`
 2. Download `bcprov-jdk15on-N.jar` and `bcpkix-jdk15on-N.jar` from the BouncyCastle releases page, where N is the
 latest release version (158 at the time of writing).
 3. Copy the downloaded JARs to the JVM extensions folder: `cp bcprov-jdk15on-158.jar bcpkix-jdk15on-158.jar $JAVA_HOME/jre/lib/ext`

### Unlimited Encryption Requirements
Using stronger encryption modes (192 and 256-bit) with the Oracle and Azul JVMs before version 8u152 requires installation of the
[Java Cryptography Extensions](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) for Oracle JVMs and the [Zulu Cryptography Extension Kit](https://www.azul.com/products/zulu-and-zulu-enterprise/zulu-cryptography-extension-kit/) for Azul JVMs. 

Note that this is not required under the following circumstances:
 - OpenJDK.
 - Java version greater than [8u161](http://www.oracle.com/technetwork/java/javase/8u161-relnotes-4021379.html#JDK-8170157)
 - Java version greater than [8u152](http://www.oracle.com/technetwork/java/javase/8u161-relnotes-4021379.html#JDK-8170157) and setting the `crypto.policy` system property to `unlimited`.

### Using Maven
Add the latest java-manta-client dependency to your Maven `pom.xml`.

```xml
<dependency>
    <groupId>com.joyent.manta</groupId>
    <artifactId>java-manta-client</artifactId>
    <version>LATEST</version>
</dependency>
```

Consult [Maven Central search](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.joyent.manta%22%20AND%20a%3A%22java-manta-client%22)
for a list of all available versions and dependency strings for other project types.

Note: Users are expected to use the same version across sub-packages, e.g. using
`com.joyent.manta:java-manta-client:3.0.0` with
`com.joyent.manta:java-manta-client-kryo-serialization:3.1.0` is not supported.

#### Minimizing bundled dependencies

A separate artifact published as `java-manta-client-unshaded` can optionally be used in place of `java-manta-client` if
users want precise control over dependency resolution. This is only recommended for users comfortable with
debugging Maven dependency resolution and usage of [Maven Enforcer's Dependency Convergence
Rule](http://maven.apache.org/enforcer/enforcer-rules/dependencyConvergence.html) is *strongly* encouraged.

## From Source
If you prefer to build from source, you'll also need
[Maven](https://maven.apache.org/), and then invoke:

``` bash
$ mvn package
```

If you want to skip running of the test suite use the `-DskipTests` property, e.g. `mvn -DskipTests package`.

# Configuration

Configuration parameters can be set through either system properties or the environment. The following classes allow for
flexible combination of parameters from different sources:

  - `StandardConfigContext` leaves all values unset and provides named setters
  - `EnvVarConfigContext` reads values from environment variables (e.g. `export MANTA_URL=value`)
  - `SystemSettingsConfigContext` reads values from system properties (e.g. from flags `-Dmanta.url=value` or a `.properties` file)
  - `ChainedConfigContext` allows the combination of multiple `ConfigContext` values
  - `AuthAwareConfigContext` initialized from the `ConfigContext` provided to `MantaClient` and manages derived objects.
    Users can directly pass in a `AuthAwareConfigContext` if they wish to change configuration parameters after client
    initialization using the `reload()` method. See
    [the JavaDoc](/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/AuthAwareConfigContext.java) for more
    information about using this class.

## Parameters

Below is a table of available configuration parameters followed by detailed descriptions of their usage.

| System Property                    | Environment Variable           | Default                              | Supports Dynamic Updates |
|------------------------------------|--------------------------------|--------------------------------------|--------------------------|
| manta.url                          | MANTA_URL                      | https://us-east.manta.joyent.com:443 | Y*                       |
| manta.user                         | MANTA_USER                     |                                      | Y*                       |
| manta.key_id                       | MANTA_KEY_ID                   |                                      | Y*                       |
| manta.key_path                     | MANTA_KEY_PATH                 | $HOME/.ssh/id_rsa (if exists)        | Y*                       |
| manta.key_content                  | MANTA_KEY_CONTENT              |                                      | Y*                       |
| manta.password                     | MANTA_PASSWORD                 |                                      | Y*                       |
| manta.no_auth                      | MANTA_NO_AUTH                  | false                                | Y*                       |
| manta.disable_native_sigs          | MANTA_NO_NATIVE_SIGS           | false                                | Y*                       |
| manta.verify_uploads               | MANTA_VERIFY_UPLOADS           | true                                 | Y                        |
| manta.timeout                      | MANTA_TIMEOUT                  | 4000                                 |                          |
| manta.retries                      | MANTA_HTTP_RETRIES             | 3 (6 for integration tests)          | Y                        |
| manta.max_connections              | MANTA_MAX_CONNS                | 24                                   |                          |
| manta.http_buffer_size             | MANTA_HTTP_BUFFER_SIZE         | 4096                                 |                          |
| https.protocols                    | MANTA_HTTPS_PROTOCOLS          | TLSv1.2                              |                          |
| https.cipherSuites                 | MANTA_HTTPS_CIPHERS            | value too big - [see code](/java-manta-client/src/main/java/com/joyent/manta/config/DefaultsConfigContext.java#L78) | |
| manta.tcp_socket_timeout           | MANTA_TCP_SOCKET_TIMEOUT       | 20000                                |                          |
| manta.connection_request_timeout   | MANTA_CONNECTION_REQUEST_TIMEOUT | 1000                               |                          |
| manta.expect_continue_timeout      | MANTA_EXPECT_CONTINUE_TIMEOUT  |                                      |                          |
| manta.upload_buffer_size           | MANTA_UPLOAD_BUFFER_SIZE       | 16384                                |                          |
| manta.skip_directory_depth         | MANTA_SKIP_DIRECTORY_DEPTH     |                                      |                          |
| manta.prune_empty_parent_depth     | MANTA_PRUNE_EMPTY_PARENT_DEPTH |                                     |                          |
| manta.download_continuations       | MANTA_DOWNLOAD_CONTINUATIONS   | 0                                    |                          |
| manta.metric_reporter.mode         | MANTA_METRIC_REPORTER_MODE     |                                      |                          |
| manta.metric_reporter.output_interval | MANTA_METRIC_REPORTER_OUTPUT_INTERVAL |                            |                          |
| manta.client_encryption            | MANTA_CLIENT_ENCRYPTION        | false                                |                          |
| manta.content_type_detection       | MANTA_CONTENT_TYPE_DETECTION   | true                                 |                          |
| manta.encryption_key_id            | MANTA_CLIENT_ENCRYPTION_KEY_ID |                                      |                          |
| manta.encryption_algorithm         | MANTA_ENCRYPTION_ALGORITHM     | AES128/CTR/NoPadding                 |                          |
| manta.permit_unencrypted_downloads | MANTA_UNENCRYPTED_DOWNLOADS    | false                                |                          |
| manta.encryption_auth_mode         | MANTA_ENCRYPTION_AUTH_MODE     | Mandatory                            |                          |
| manta.encryption_key_path          | MANTA_ENCRYPTION_KEY_PATH      |                                      |                          |
| manta.encryption_key_bytes         |                                |                                      |                          |
| manta.encryption_key_bytes_base64  | MANTA_ENCRYPTION_KEY_BYTES     |                                      |                          |

Note: Dynamic Updates marked with an asterisk (*) are enabled by the `AuthAwareConfigContext` class.

* `manta.url` ( **MANTA_URL** )
    The URL of the Manta service endpoint.
* `manta.user` ( **MANTA_USER** )
    The account name used to access the manta service. If accessing via a [subuser](https://docs.joyent.com/public-cloud/rbac/users),
    you will specify the account name as "user/subuser".
* `manta.key_id`: ( **MANTA_KEY_ID**)
    The fingerprint for the public key used to access the manta service. Can be retrieved using `ssh-keygen -l -f ${MANTA_KEY_PATH} -E md5 | cut -d' ' -f 2`
* `manta.key_path` ( **MANTA_KEY_PATH**)
    The name of the file that will be loaded for the account used to access the manta service.
* `manta.key_content` ( **MANTA_KEY_CONTENT**)
    The content of the private key as a string. This is an alternative to `manta.key_path`. Both
    `manta.key_path` and can't be specified at the same time `manta.key_content`.
* `manta.password` ( **MANTA_PASSWORD**)
    The password associated with the key specified. This is optional and not normally needed.
* `manta.no_auth` (**MANTA_NO_AUTH**)
    When set to true, this disables HTTP Signature authentication entirely. This is
    only really useful when you are running the library as part of a Manta job.
* `manta.disable_native_sigs` (**MANTA_NO_NATIVE_SIGS**)
    When set to true, this disables the use of native code libraries for cryptography.
* `manta.verify_uploads` (**MANTA_VERIFY_UPLOADS**)
    When set to true, the client calculates a MD5 checksum of the file being uploaded
    to Manta and then checks it against the result returned by Manta.
* `manta.timeout` ( **MANTA_TIMEOUT**)
    The number of milliseconds to wait until giving up when trying to make a new connection
    to Manta.
* `manta.retries` ( **MANTA_HTTP_RETRIES**)
    The number of times to retry failed HTTP requests. Setting this value to zero disables retries completely.
* `manta.max_connections` ( **MANTA_MAX_CONNS**)
    The maximum number of open HTTP connections to the Manta API.
* `manta.http_buffer_size` (**MANTA_HTTP_BUFFER_SIZE**)
    The size of the buffer to allocate when processing streaming HTTP data. This sets the value
    used by Apache HTTP Client `SessionInputBufferImpl` implementation. Ranges from 1024-16384
    are acceptable depending on your average object size and streaming needs.
* `https.protocols` (**MANTA_HTTPS_PROTOCOLS**)
    A comma delimited list of TLS protocols.
* `https.cipherSuites` (**MANTA_HTTPS_CIPHERS**)
    A comma delimited list of TLS cipher suites.
* `manta.tcp_socket_timeout` (**MANTA_TCP_SOCKET_TIMEOUT**)
    Time in milliseconds to wait for TCP socket's blocking operations - zero means wait forever.
* `manta.connection_request_timeout` (**MANTA_CONNECTION_REQUEST_TIMEOUT**)
    Time in milliseconds to wait for a connection from the connection pool.
* `manta.expect_continue_timeout` (**MANTA_EXPECT_CONTINUE_TIMEOUT**)
    Nullable integer indicating the number of milliseconds to wait for a response from the server before
    sending the request body. If enabled, the recommended wait time is **3000** ms based on the default defined in
    `HttpRequestExecutor.DEFAULT_WAIT_FOR_CONTINUE`. Enabling this setting can
    improve response latencies and error visibility when the server is under high load at the expense
    of potentially decreased total throughput. We recommend benchmarking with different values for this option before
    enabling it for production use.
* `manta.upload_buffer_size` (**MANTA_UPLOAD_BUFFER_SIZE**)
    The initial amount of bytes to attempt to load into memory when uploading a stream. If the
    entirety of the stream fits within the number of bytes of this value, then the
    contents of the buffer are directly uploaded to Manta in a retryable form.
* `manta.skip_directory_depth` (**MANTA_SKIP_DIRECTORY_DEPTH**)
    Integer indicating the number of **non-system** directory levels to attempt to skip for recursive `putDirectory`
    operation (i.e. `/$MANTA_USER` and `/$MANTA_USER/stor` would not be counted). A detailed explanation and example are provided [later in this document](/USAGE.md#skipping-directories)
* `manta.prune_empty_parent_depth` (**MANTA_PRUNE_EMPTY_PARENT_DEPTH**)
    Integer indicating the maximum number of empty parent directories to prune. If the client is given the value of -1 then the client will try and delete
    all empty parent directories. see [the section on parent directory pruning later in this document](/USAGE.md#pruning-empty-parent-directories) 
* `manta.download_continuations` (**MANTA_DOWNLOAD_CONTINUATIONS**)
    Nullable Integer property for enabling the [download continuation](#download-continuation) "optimization."
    A value of 0 explicitly disabled this feature, a value of `-1` enables unlimited continuations, a positive value
    sets the maximum number of continuations that may be delivered for a single request/reponse.
* `manta.metric_reporter.mode` (**MANTA_METRIC_REPORTER_MODE**)
    Enum type indicating how metrics should be reported. Options include `DISABLED`, `JMX`, and `SLF4J`. Leaving this value
    unset or selecting `DISABLED` will prevent the client from gathering and reporting metrics. Certain reporters
    (only `SLF4J` at present) requires also setting an output interval.
    See [the section on monitoring](#monitoring) for more information about reporting modes.
* `manta.metric_reporter.output_interval` (**MANTA_METRIC_REPORTER_OUTPUT_INTERVAL**)
    Nullable integer interval in seconds at which metrics are reported by periodic reporters.
    This number must be set and greater than zero if `manta.metric_reporter.mode`/`MANTA_METRIC_REPORTER_MODE`
    is set to `SLF4J`. Defaults to `null`.
* `manta.client_encryption` (**MANTA_CLIENT_ENCRYPTION**)
    Boolean indicating if client-side encryption is enabled.
* `manta.content_type_detection` (**MANTA_CONTENT_TYPE_DETECTION**)
    Boolean indicating if automatic content-type detection while uploading a file in Manta is enabled.    
* `manta.encryption_key_id` (**MANTA_CLIENT_ENCRYPTION_KEY_ID**)
    Unique ID of the client-side encryption key being used. It must be in US-ASCII
    using only printable characters and with no whitespace.
* `manta.encryption_algorithm` (**MANTA_ENCRYPTION_ALGORITHM**)
    The client-side encryption algorithm used to encrypt and decrypt data. Valid
    values are listed in [SupportedCipherDetails](java-manta-client/src/main/java/com/joyent/manta/client/crypto/SupportedCipherDetails.java#L26).
* `manta.permit_unencrypted_downloads` (**MANTA_UNENCRYPTED_DOWNLOADS**)
    Boolean indicating that unencrypted files can be downloaded when client-side
    encryption is enabled.
* `manta.encryption_auth_mode` (**MANTA_ENCRYPTION_AUTH_MODE**)
    [EncryptionAuthenticationMode](/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/EncryptionAuthenticationMode.java)
    enum type indicating that authenticating encryption verification is either `Mandatory`, `Optional` or `VerificationDisabled`.
* `manta.encryption_key_path` (**MANTA_ENCRYPTION_KEY_PATH**)
    The path on the local filesystem or a URI understandable by the JVM indicating the
    location of the private key used to perform client-side encryption. If this value is
    non-null, then no other encryption key values can be non-null.
* `manta.encryption_key_bytes`
    The private key used to perform client-side encryption as a byte array. If this value is
    non-null, then no other encryption key values can be non-null.
* `manta.encryption_key_bytes_base64` (**MANTA_ENCRYPTION_KEY_BYTES**)
    The private key used to perform client-side encryption encoded as a base64 string.
    If this value is non-null, then no other encryption key values can be non-null.

See the examples for a [typical configuration using defaults](/java-manta-examples/src/main/java/SimpleClient.java) and another
[configuration with client-side encryption enabled](/java-manta-examples/src/main/java/SimpleClientEncryption.java)

## Client-side Encryption

In order to enable client side encryption for downloading and decrypting encrypted files, please set the following
system properties. Please consult the [Configuration Parameters list](/USAGE.md#parameters) for the corresponding
environment variable.

- `manta.client_encryption` - set to `true`
- `manta.encryption_key_bytes_base64` or `manta.encryption_key_bytes`

Additionally, you should set the following system properties to support encrypting and uploading files using client-side
encryption.

- `manta.encryption_algorithm`
- `manta.encryption_key_id`

Below is a table of each of the supported encryption algorithms and the features they provide. Additional information
about how these encryption modes differ is available [here](https://en.wikipedia.org/wiki/Block_cipher_mode_of_operation)
and [here](https://security.stackexchange.com/questions/52665/which-is-the-best-cipher-mode-and-padding-mode-for-aes-encryption).

| Algorithm                            | Range Requests                     | Authenticated Encryption       |
|--------------------------------------|------------------------------------|--------------------------------|
| AES128/GCM/NoPadding                 | No                                 | Yes                            |
| AES192/GCM/NoPadding                 | No                                 | Yes                            |
| AES256/GCM/NoPadding                 | No                                 | Yes                            |
|                                      |                                    |                                |
| AES128/CTR/NoPadding                 | Yes                                | No                             |
| AES192/CTR/NoPadding                 | Yes                                | No                             |
| AES256/CTR/NoPadding                 | Yes                                | No                             |
|                                      |                                    |                                |
| AES128/CBC/PKCS5Padding              | No                                 | No                             |
| AES192/CBC/PKCS5Padding              | No                                 | No                             |
| AES256/CBC/PKCS5Padding              | No                                 | No                             |

*Note* that all algorithms will honor the `manta.encryption_auth_mode` setting.

*Note* that any non-authenticated encryption algorithm will have an
[HMAC](https://en.wikipedia.org/wiki/Hash-based_message_authentication_code) generated to authenticate that ciphertext
was not modified.

*Note* each instance of a MantaClient will only support the encryption/decryption for the algorithm configured for that instance.

### Improving Encryption Performance

## Automated Content-Type Detection

While uploading any file in Manta using the Java SDK, depending on the data type, Users could opt to disable a particular 
feature of the SDK which enables it to internally detect the HTTP content-type of any given file or stream. The following 
system properties are configured for this purpose. Please consult the [Configuration Parameters list](/USAGE.md#parameters) 
for the corresponding environment variable.

- `manta.content_type_detection` - set to `false`

#### Enabling libnss Support via PKCS11

[NSS](https://developer.mozilla.org/en-US/docs/Mozilla/Projects/NSS) can be used with
[PKCS11](https://en.wikipedia.org/wiki/PKCS_11) to provide a native code interface for encryption
functions. The SDK will detect if libnss is installed via PKCS11 and prefer it over
the Legion of the Bouncy Castle library if it is available.

To add libnss support to your JVM, you will need to locate libnss on your system. Then you will
need to add a configuration file that will be referenced by your JVM. There is documentation
on the install process [available online](http://docs.oracle.com/javase/8/docs/technotes/guides/security/p11guide.html),
but not much guidance on what to do per distro / operating system.

Below is a list of distros / operating systems and the packages and locations of
libnss.

**Ubuntu**
Debian Package: `libnss3`
Library Location: /usr/lib/x86_64-linux-gnu

**CentOS**
Yum Package: `nss`
Library Location: /usr/lib64

**MacOS**
Homebrew Package: `nss`
Library Location: /usr/local/opt/nss/lib

**SmartOS**
Pkgsrc Package: `nss`
Library Location: /opt/local/lib/nss

Once you have installed libnss and have located it's path, you will need to add a configuration
file to your system. The path doesn't matter, but for the example's sake, we will give it a
path of `/etc/nss.cfg`.

The file would have the following contents if you were on Ubuntu:
```
name = NSS
nssLibraryDirectory = /usr/lib/x86_64-linux-gnu
nssDbMode = noDb
attributes = compatibility
```

Make sure that the name field is `NSS` because the SDK will only use the library if that specific
name is set. Next, edit the following file: `$JAVA_HOME/jre/lib/security/java.security`

Find the lines specifying security providers. It should look something like:
```
security.provider.1=sun.security.provider.Sun
security.provider.2=sun.security.rsa.SunRsaSign
security.provider.3=sun.security.ec.SunEC
security.provider.4=com.sun.net.ssl.internal.ssl.Provider
security.provider.5=com.sun.crypto.provider.SunJCE
security.provider.6=sun.security.jgss.SunProvider
security.provider.7=com.sun.security.sasl.Provider
security.provider.8=org.jcp.xml.dsig.internal.dom.XMLDSigRI
security.provider.9=sun.security.smartcardio.SunPCSC
```

Now, add a line in front of the first provider and make it provider number one,
then appropriately increment the other providers:

```
security.provider.1=sun.security.pkcs11.SunPKCS11 /etc/nss.cfg
security.provider.2=sun.security.provider.Sun
security.provider.3=sun.security.rsa.SunRsaSign
security.provider.4=sun.security.ec.SunEC
security.provider.5=com.sun.net.ssl.internal.ssl.Provider
security.provider.6=com.sun.crypto.provider.SunJCE
security.provider.7=sun.security.jgss.SunProvider
security.provider.8=com.sun.security.sasl.Provider
security.provider.9=org.jcp.xml.dsig.internal.dom.XMLDSigRI
security.provider.10=sun.security.smartcardio.SunPCSC
```

Once this is complete, you should now have libnss providing your cryptographic
functions.

#### Overriding Default Cryptographic Provider Ranking

The default ranking of cryptographic providers within the SDK is as follows:

Non-cloneable implementations:
 * NSS via PKCS11 ("SunPKCS11-NSS")
 * Bouncy Castle ("BC")
 * Sun JCE provider ("SunJCE")

Cloneable implementations:
 * Bouncy Castle ("BC")
 
The ranking of preferred providers can be changed using the Java system property
`manta.preferred.security.providers`. The provider names are specified in a comma
separated format. For example, in order to have the system prefer SunJCE, BC and
then SunPKCS11-NSS in that order, you would specify:

```
java <...> -Dmanta.preferred.security.providers=SunJCE,BC,SunPKCS11-NSS
```  

You may want to change the ranking if you are unable to use the libnss provider
and still want the performance benefits of AES-NI via the SunJCE provider.

#### Enabling Native FastMD5 Support

The Java Manta SDK uses [Timothy W Macinta's Fast MD5](http://www.twmacinta.com/myjava/fast_md5.php)
implementation internally to perform MD5 checksum operations. By default, the
SDK uses the pure Java MD5 implementation which is faster than the default
JDK implementation for large amounts of data. If the default performance
provided by the SDK is insufficient, native MD5 implementation libraries can
be loaded via JNI.

To get a native library for your system, [download the library](http://www.twmacinta.com/myjava/fast_md5.php#download) and choose the share object library that is appropriate for
your system (hint: they are contained in the `./build/` directory). The easiest
way to get up and running is to copy the library to a path that makes sense
for your application and provide a path to the library using the `com.twmacinta.util.MD5.NATIVE_LIB_FILE` system
property. For example, on amd64 architecture running linux and assuming the contents
have been extracted to `/opt/myapp/lib` you would use the following invocation
to run your application with native MD5 support:

```
$ java -Dcom.twmacinta.util.MD5.NATIVE_LIB_FILE=/opt/myapp/lib/arch/linux_amd64/MD5.so -jar /opt/myapp/app.jar
```

You can also place multiple files in a directory structure and have the FastMD5
library automatically choose the right library for your platform. The details
for getting are best described in the [FastMD5 Javadocs](http://www.twmacinta.com/myjava/fast_md5_javadocs/).

### Logging

The SDK utilizes [slf4j](http://www.slf4j.org/), and logging can be configured
using a SLF4J implementation. The `java-manta-client` artifact includes an [Apache Commons Logger adaptor to
SLF4J](https://www.slf4j.org/legacy.html#jclOverSLF4J) so Apache HTTP Client logs
will also be output via SLF4J.

When configuring logging keep in mind the [package relocations](/java-manta-client/pom.xml#L100)
being performed. Consumers which depend on `java-manta-client-unshaded` should use the package name
as is, but consumers which depend on the shaded artifact (`java-manta-client`) should use the
relocated package name. For example, if your project is using `ch.qos.logback:logback-classic`
and you wish to debug the establishment and leasing of HTTP connections:

| Dependency                    | Logback Logger Configuration                                                  |
|-------------------------------|-------------------------------------------------------------------------------|
| java-manta-client             | `<logger name="com.joyent.manta.org.apache.http.impl.conn" level="DEBUG" />`  |
| java-manta-client-unshaded    | `<logger name="org.apache.http.impl.conn" level="DEBUG" />`                   |

Please note that the Commons Logger adaptor is not a dependency of `java-manta-client-unshaded` and it is the user's
responsibility to add their own dependency if they wish to collect Apache HttpClient logs. For more information on log
bridging in SLF4J please review [this page](https://www.slf4j.org/legacy.html).

### Monitoring

Users can enable monitoring in order to provide better visibility into behavior and performance of a `MantaClient`.
This requires selecting a reporting mode using the following settings:

- `manta.metric_reporter.mode` select the method by which metrics are exported. Unset by default. Options include:
    - `DISABLED`: explicitly disables monitoring.
    - `JMX`: registers MBeans in [JMX](https://en.wikipedia.org/wiki/Java_Management_Extensions). MBeans are created
    for the following at present in addition to each metric listed below:
        - `ConfigContextMBean` displays the `ConfigContext` settings used to build the client.
    - `SLF4J`: reporters metrics through the generic logging interface provided by [SLF4J](http://www.slf4j.org/).
    This setting requires users to also supply a reporting output interval.
- `manta.metric_reporter.output_interval` specify the amount of time in seconds between reporting metrics for
    periodic reporters. Required by `SLF4J`. Setting this value too low may lead to excessive disk usage. A value of
    60 affords minute-by-minute granularity in combination with the 1-minute moving average provided
    by certain metric values. Logging is done at the `INFO` level using a logger named
    `com.joyent.manta.client.metrics`. An example metric output for client ID `c16a2f85-90f7-4e7c-b0f3-b8993eca18d1`
    would look like the following (newlines added for clarity):
    ```
    [metrics-logger-reporter-1-thread-1] INFO  com.joyent.manta.client.metrics [ ] -
    type=METER,
    name=c16a2f85-90f7-4e7c-b0f3-b8993eca18d1.retries,
    count=2,
    mean_rate=0.07982106952096357,
    m1=0.028248726311583667,
    m5=0.006448405864180696,
    m15=0.0021976788366558607,
    rate_unit=events/second
    ```

The full list of metrics exported by the client (available through both JMX and SLF4J) is as follows:
- `requests-$METHOD`: A [timer](http://metrics.dropwizard.io/4.0.0/manual/core.html#timer) for each request
    method measuring the rate, and timings (with percentiles) of HTTP requests. Example values for `$METHOD` include
    `GET`, `PUT`, `DELETE`, etc.
- `exceptions-$CLASS`: A [meter](http://metrics.dropwizard.io/4.0.0/manual/core.html#meters) for each
    exception which occurred while executing HTTP requests measuring the rates and counts. Example values for
    `$CLASS` include `SocketTimeoutException`, `InterruptedIOException`, etc.
- `connections-$CLASSIFICATION`: A set of [guages](https://metrics.dropwizard.io/4.0.0/manual/core.html#gauges) which
    expose the state of the connection pool. `$CLASSIFICATION` is one of `available`, `leased`, `max`, `pending` and
    corresponds to the[similarly named HttpClient PoolStats
    fields](http://hc.apache.org/httpcomponents-core-ga/httpcore/apidocs/org/apache/http/pool/PoolStats.html).
- `retries`: A [meter](http://metrics.dropwizard.io/4.0.0/manual/core.html#meters) measuring the rate
    and count of retries the client has attempted, in addition to 1-, 5-, and 15-minute moving averages.
- `get-continuations-recovered-$CLASS`: A [counter](https://metrics.dropwizard.io/4.0.0/manual/core.html#counters)
    tracking the number of exceptions by exception class from which [download continuators](#download-continuation)
    have recovered. Any non-zero values recorded in these counters indicate that download continuation is
    being used to mitigate network failures.
- `get-continuations-per-request-distribution`: A
    [histogram](https://metrics.dropwizard.io/4.0.0/manual/core.html#histograms) tracking the distribution of
    continuations served per request. Since each continuator only handles a single logical request and the
    wrapping `InputStream` signals closure to the continuator it can record how many times it was invoked for a single
    logical download. Any non-zero values recorded in this histogram indicate that download continuation is
    being used to mitigate network failures.



### Customizing the client further

It is possible to supply an `HttpClientBuilder` in order to further customize the behavior of a `MantaClient` instance.
Users leveraging this feature should be comfortable with the internals of the Apache HttpClient library and
familiarity with the
[`MantaConnectionFactory`](/java-manta-client-unshaded/src/main/java/com/joyent/manta/http/MantaConnectionFactory.java)
class is recommended.

### Modifying configuration after client instantiation

Some configuration parameters can be updated in the `ConfigContext` after the client has already been constructed.
Instead of directly providing a `ConfigContext` users can instead supply a `AuthAwareConfigContext` to the `MantaClient`
constructor which can be programmatically updated using its `reload()` method. Because the `MantaClient` can be used in a
multi-threaded fashion, users should be careful to await termination of in-flight requests in before triggering authentication reload.
Users should be aware that it is also possible to construct a `MantaClient` instance using an incomplete configuration when utilizing
an `AuthAwareConfigContext` by disabling authentication until a private key can be set.
Concurrently updating configuration values while requests are still pending can lead to errors and unpredictable results.
See the [Dynamic Authentication example](/java-manta-examples/src/main/java/DynamicAuthentication.java) and
[this test case](/java-manta-it/src/test/java/com/joyent/manta/client/MantaClientAuthenticationChangeIT) for example usage.

### Skipping directories

In order to ease migration from other object stores which do not treat directories as first-class entities a method for creating arbitrarily-nested directories is provided by the [MantaClient#putDirectory(String, boolean)](http://static.javadoc.io/com.joyent.manta/java-manta-client/3.2.1/com/joyent/manta/client/MantaClient.html#putDirectory-java.lang.String-boolean-) method. This can carry a high performance cost unless used judiciously so an optional performance enhancement is provided in the form of the `manta.skip_directory_depth`/`MANTA_SKIP_DIRECTORY_DEPTH` setting. This setting indicates how many intermediate **user-writeable** directories (i.e. those which are not "top-level directories") the client can assume to already exist. Since the first two levels of a directory path are managed by Manta they are not considered for this optimization (since they _must_ exist). To illustrate this feature let's look at a few examples:

#### Scenario 1, optimization disabled
- `manta.skip_directory_depth` = `0`
- directory path = `"/$MANTA_USER/stor/foo/bar/baz"`
- result:
  - writeable segments = 3
    - `.../foo`
    - `.../foo/bar`
    - `.../foo/bar/baz`
  - strategy:
    - standard (because setting is disabled)
  - requests sent:
    - `PUT /$MANTA_USER/stor/foo`
    - `PUT /$MANTA_USER/stor/foo/bar`
    - `PUT /$MANTA_USER/stor/foo/bar/baz`

#### Scenario 2, optimization enabled
- `manta.skip_directory_depth` = `2`
- directory path = `"/$MANTA_USER/stor/foo/bar/baz"`
- result:
  - writeable segments = 3
    - `.../foo`
    - `.../foo/bar`
    - `.../foo/bar/baz`
  - strategy:
    - skip, assume first two paths already exist
  - requests sent:
    - `PUT /$MANTA_USER/stor/foo/bar/baz`

#### Scenario 3, optimization enabled, longer path
- `manta.skip_directory_depth` = `2`
- directory path = `"/$MANTA_USER/stor/foo/bar/baz/subdir0/subdir1"`
- result:
  - writeable segments = 5
    - `.../foo`
    - `.../foo/bar`
    - `.../foo/bar/baz`
    - `.../foo/bar/baz/subdir0`
    - `.../foo/bar/baz/subdir0/subdir1`
  - strategy:
    - skip, assume first two paths already exist
  - requests sent:
    - `PUT /$MANTA_USER/stor/foo/bar/baz`
    - `PUT /$MANTA_USER/stor/foo/bar/baz/subdir0`
    - `PUT /$MANTA_USER/stor/foo/bar/baz/subdir0/subdir1`

#### Scenario 4, **error case**, optimization set too high
- `manta.skip_directory_depth` = `2`
- directory path = `"/$MANTA_USER/stor/foo/bar/baz"`
- result:
  - writeable segments = 3
    - `.../foo`
    - `.../foo/bar`
    - `.../foo/bar/baz`
  - strategy:
    - skip, assume first two paths already exist
  - requests sent:
    - `PUT /$MANTA_USER/stor/foo/bar/baz`
      - fails because neither `/$MANTA_USER/stor/foo` nor `/$MANTA_USER/stor/foo/bar` exist
      - optimization disabled, revert to standard creation order
    - `PUT /$MANTA_USER/stor/foo`
    - `PUT /$MANTA_USER/stor/foo/bar`
    - `PUT /$MANTA_USER/stor/foo/bar/baz`

#### Scenario 5, optimization enabled, requested directory with less segments than setting
- `manta.skip_directory_depth` = `5`
- directory path = `"/$MANTA_USER/stor/foo/bar/baz"`
- result:
  - writeable segments = 3
    - `.../foo`
    - `.../foo/bar`
    - `.../foo/bar/baz`
  - strategy:
    - standard, because there are less segments than the skip depth*
  - requests sent:
    - `PUT /$MANTA_USER/stor/foo`
    - `PUT /$MANTA_USER/stor/foo/bar`
    - `PUT /$MANTA_USER/stor/foo/bar/baz`

\* Note that in Scenario 4 where the setting is more aggressive than needed, the current behavior is to fall back to creating all intermediate directories. This situation is being revisited in [#414](https://github.com/joyent/java-manta/issues/414)

### Pruning empty parent directories

When `manta.prune_empty_parent_depth`/`MANTA_PRUNE_EMPTY_PARENT_DEPTH` is set to `-1` or a positive value, when deleting an object in Manta, either a file or a directory, the client will attempt to delete parent directories that are empty. If a positive integer is supplied the client will only try to delete up to the number supplied. If -1 is given the client will try to delete in the path until it finds a directory that it can not delete.  

#### Scenario 1 : Prune Empty Parent Depth positive int
- Given the directory structure : 
   /Dir1
   /Dir1/Dir2
   /Dir1/Dir2/Dir3
   /Dir1/Dir2/Dir3/Dir4
   /Dir1/Dir2/Dir3/Dir4/test.txt
   If you have prune_empty_parent_depth set to 1 then delete test.txt, the client should delete Dir4 as well.
   If you have prune_empty_parent_depth set to 2 the client should delete Dir3, with the directories and file in the previous case.
   If you have prune_empty_parent_depth set to 3 the client should delete Dir2, with the directories and file cumulatively included from the previous cases.
   If you have prune_empty_parent_depth set to 4 the client should delete Dir1, with the directories and file cumulatively included from the previous cases. 
#### Scenario 2 : Prune Empty Parent Depth -1
- Given the directory structure : 
   /Dir1
   /Dir1/DirA
   /Dir1/Dir2
   /Dir1/Dir2/Dir3
   /Dir1/Dir2/Dir3/test.txt
   In this case, if test.txt is the target with the prune value set to -1 then test.txt, Dir3, and Dir2 will be deleted. The client will stop on Dir1 because it will not be empty, it still has a child of DirA.  	  


### Download continuation

When `manta.download_continuation`/`MANTA_DOWNLOAD_CONTINUATION` is set to `-1` or a positive value the client's HTTP helper class
will check that the initial request/response for a GET request passes validation (described below) and will wrap the
returned stream in with the information and helper classes needed to automatically resume a download. This is similar to
automatic retries in that the client will automatically issue an HTTP request on behalf of the user but is currently resctricted 
to GET requests only. In order to implement transparent swapping of source streams we wrap the content of the initial HTTP
response in a pluggable `InputStream` which satisfies the following conditions:
 - accepts an `InputStreamContinuator` which can produce new `InputStream`s to the same data source
 - delegates reads to the underlying `InputStream`
 - if a non-fatal exception is encountered, the current `InputStream` is closed and a new stream generated by the embedded
 `InputStreamContinuator` is used instead

Since this behaves similarly to the HttpClient retry behavior we also check that the `IOException` encountered while reading
from the source stream is merely a transient error (e.g. `SocketTimeoutException` is non-fatal, `UnknownHostException` is unlikely
but fatal). If the remote object has changed (either because the `ETag` or `Content-Length`/`Content-Range` changed) then 
the encountered exception will be rethrown. If the caller indicates that they have read an invalid number of bytes (e.g. somehow
total bytes read decreases) the continuator will complain about the impossible situation rethrow the encountered exception.

Validation of the initial request/response includes:
 - correct response code (200 for non-range requests, 206 for range requests)
 - requests including a single `If-Match` header (only one is supported) should have the same value in the response's
 `ETag` header
 - requests including a single `Range` header (only one is supported) should have an appropriate value in the response's
 `Content-Range` header

No enhancement will be performed if any of the following conditions are met:
 - the request is not a GET request
 - the request headers were malformed or multi-valued
 - the user passed in their own `HttpClientBuilder` through `MantaConnectionFactoryConfigurator` so we can't be sure
 retry cancellation is supported

We may pick up and validate additional response headers in the future (e.g. validate that `Content-MD5` never changes).
