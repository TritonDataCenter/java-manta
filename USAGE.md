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
Using stronger encryption modes (192 and 256-bit) will require installation of the
[Java Cryptography Extensions](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html).

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

## Parameters

Below is a table of available configuration parameters followed by detailed descriptions of their usage.

| System Property                    | Environment Variable           | Default                              |
|------------------------------------|--------------------------------|--------------------------------------|
| manta.url                          | MANTA_URL                      | https://us-east.manta.joyent.com:443 |
| manta.user                         | MANTA_USER                     |                                      |
| manta.key_id                       | MANTA_KEY_ID                   |                                      |
| manta.key_path                     | MANTA_KEY_PATH                 | $HOME/.ssh/id_rsa                    |
| manta.key_content                  | MANTA_KEY_CONTENT              |                                      |
| manta.password                     | MANTA_PASSWORD                 |                                      |
| manta.timeout                      | MANTA_TIMEOUT                  | 20000                                |
| manta.retries                      | MANTA_HTTP_RETRIES             | 3 (6 for integration tests)          |
| manta.max_connections              | MANTA_MAX_CONNS                | 24                                   |
| manta.http_buffer_size             | MANTA_HTTP_BUFFER_SIZE         | 4096                                 |
| https.protocols                    | MANTA_HTTPS_PROTOCOLS          | TLSv1.2                              |
| https.cipherSuites                 | MANTA_HTTPS_CIPHERS            | value too big - [see code](/java-manta-client/src/main/java/com/joyent/manta/config/DefaultsConfigContext.java#L78) |
| manta.no_auth                      | MANTA_NO_AUTH                  | false                                |
| manta.disable_native_sigs          | MANTA_NO_NATIVE_SIGS           | false                                |
| manta.tcp_socket_timeout           | MANTA_TCP_SOCKET_TIMEOUT       | 10000                                |
| manta.verify_uploads               | MANTA_VERIFY_UPLOADS           | true                                 |
| manta.upload_buffer_size           | MANTA_UPLOAD_BUFFER_SIZE       | 16384                                |
| manta.client_encryption            | MANTA_CLIENT_ENCRYPTION        | false                                |
| manta.encryption_key_id            | MANTA_CLIENT_ENCRYPTION_KEY_ID |                                      |
| manta.encryption_algorithm         | MANTA_ENCRYPTION_ALGORITHM     | AES128/CTR/NoPadding                 |
| manta.permit_unencrypted_downloads | MANTA_UNENCRYPTED_DOWNLOADS    | false                                |
| manta.encryption_auth_mode         | MANTA_ENCRYPTION_AUTH_MODE     | Mandatory                            |
| manta.encryption_key_path          | MANTA_ENCRYPTION_KEY_PATH      |                                      |
| manta.encryption_key_bytes         |                                |                                      |
| manta.encryption_key_bytes_base64  | MANTA_ENCRYPTION_KEY_BYTES     |                                      |

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
* `manta.timeout` ( **MANTA_TIMEOUT**)
The number of milliseconds to wait after a request was made to Manta before failing.
* `manta.retries` ( **MANTA_HTTP_RETRIES**)
The number of times to retry failed HTTP requests.
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
* `manta.no_auth` (**MANTA_NO_AUTH**)
When set to true, this disables HTTP Signature authentication entirely. This is
only really useful when you are running the library as part of a Manta job.
* `http.signature.native.rsa` (**MANTA_NO_NATIVE_SIGS**)
When set to true, this disables the use of native code libraries for cryptography.
* `manta.tcp_socket_timeout` (**MANTA_TCP_SOCKET_TIMEOUT**)
Time in milliseconds to wait for TCP socket's blocking operations - zero means wait forever.
* `manta.verify_uploads` (**MANTA_VERIFY_UPLOADS**)
When set to true, the client calculates a MD5 checksum of the file being uploaded
to Manta and then checks it against the result returned by Manta.
* `manta.upload_buffer_size` (**MANTA_UPLOAD_BUFFER_SIZE**)
The initial amount of bytes to attempt to load into memory when uploading a stream. If the
entirety of the stream fits within the number of bytes of this value, then the
contents of the buffer are directly uploaded to Manta in a retryable form.
* `manta.client_encryption` (**MANTA_CLIENT_ENCRYPTION**)
Boolean indicating if client-side encryption is enabled.
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
[EncryptionAuthenticationMode](java-manta-client/src/main/java/com/joyent/manta/config/EncryptionAuthenticationMode.java)
enum type indicating that authenticating encryption verification is either Mandatory or Optional.
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