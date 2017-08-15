[![Build Status](https://travis-ci.org/joyent/java-manta.svg?branch=travis)](https://travis-ci.org/joyent/java-manta)

# Java Manta Client SDK

[java-manta](http://joyent.github.com/java-manta) is a community-maintained Java
SDK for interacting with Joyent's Manta system.

## Projects Using the Java Manta Client SDK

* [Apache Commons VFS Manta Provider (Virtual File System)](https://github.com/joyent/commons-vfs-manta)
* [Hadoop Filesystem Driver for Manta](https://github.com/joyent/hadoop-manta)
* [Manta Logback Rollover](https://github.com/dekobon/manta-logback-rollover)
* [COSBench Adaptor for Manta - Object Store Benchmarks](https://github.com/joyent/cosbench-manta)

## Installation

### Requirements
* [Java 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher.
* [Maven 3.3.x](https://maven.apache.org/)
* [Java Cryptography Extension](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html)

#### CLI Requirements

Add [BouncyCastle](http://www.bouncycastle.org/latest_releases.html) as a security provider
 1. Edit "$JAVA_HOME/jre/lib/security/java.security " Add an entry for BouncyCastle  
 `security.provider.11=org.bouncycastle.jce.provider.BouncyCastleProvider`
 2. Copy bc*.jar to $JAVA_HOME/jre/lib/ext

### Using Maven
Add the latest java-manta dependency to your Maven `pom.xml`.

```xml
<dependency>
    <groupId>com.joyent.manta</groupId>
    <artifactId>java-manta-client</artifactId>
    <version>LATEST</version>
</dependency>
```

Note: Users are expected to use the same version across sub-packages, e.g. using
`com.joyent.manta:java-manta-client:3.0.0` with
`com.joyent.manta:java-manta-client-kryo-serialization:3.1.0` is not supported.

### From Source
If you prefer to build from source, you'll also need
[Maven](https://maven.apache.org/), and then invoke:

``` bash
# mvn package
```

Which will compile the jar to ./targets/java-manta-version.jar. You can then
add it as a dependency to your Java project.

If you want to skip running of the test suite, use the `-DskipTests` property.

## Configuration

Configuration can be done through both system properties or environment variables, with the environment taking
precedence over system properties. Listed below are the configuration options you'll need to get started.

| Default                              | System Property                    | Environment Variable           |
|--------------------------------------|------------------------------------|--------------------------------|
| https://us-east.manta.joyent.com:443 | manta.url                          | MANTA_URL                      |
|                                      | manta.user                         | MANTA_USER                     |
|                                      | manta.key_id                       | MANTA_KEY_ID                   |
| $HOME/.ssh/id_rsa                    | manta.key_path                     | MANTA_KEY_PATH                 |

* `manta.url` ( **MANTA_URL** )
The URL of the manta service endpoint to test against
* `manta.user` ( **MANTA_USER** )
The account name used to access the manta service. If accessing via a [subuser](https://docs.joyent.com/public-cloud/rbac/users),
you will specify the account name as "user/subuser".
* `manta.key_id`: ( **MANTA_KEY_ID**)
The fingerprint for the public key used to access the manta service.
* `manta.key_path` ( **MANTA_KEY_PATH**)
The name of the file that will be loaded for the account used to access the manta service.

Please refer to the [configuration documentation](/docs/CONFIGURING.md) for the full
list of configuration options, which include retry and performance tuning in addition to encryption and authentication
parameters.

## Accounts, Usernames and Subusers
Joyent's SmartDataCenter account implementation is such that you can have a
subuser as a dependency upon a user. This is part of SmartDataCenter's [RBAC
implementation](https://docs.joyent.com/public-cloud/rbac/users). A subuser
is a user with a unique username that is joined with the account holder's
username. Typically, this is in the format of "user/subuser".

Within the Java Manta library, we refer to the account name as the entire
string used to login - "user/subuser". When we use the term user it is in
reference to the "user" portion of the account name and when we use the term
subuser, it is in reference to the subuser portion of the account name.

The notable exception is that in the configuration passed into the library,
we have continued to use the terminology *Manta user* to refer to the
account name because of historic compatibility concerns.

## Usage

You'll need a manta login, an associated key, and its corresponding key
fingerprint.  

You will then create or use an implementation of `ConfigContext` to set up
your configuration. Once you have an instance of your configuration class,
you will then construct a `MantaClient` instance. The `MantaClient` class
is intended to be used per Manta account. It is thread-safe and you should
share one instance across multiple threads.

For detailed usage instructions consult the provided JavaDoc and examples. The
package JavaDoc can also be browsed online at [javadoc.io](https://javadoc.io/doc/com.joyent.manta/java-manta-client/)

### General Examples
 
* [Get request and client setup](/java-manta-examples/src/main/java/SimpleClient.java)
* [Multipart upload](/java-manta-examples/src/main/java/ServerMultipart.java)
* [Client-side Encryption](/java-manta-examples/src/main/java/SimpleClientEncryption.java)

### Job Examples

Jobs can be created directly with the `MantaClient` class or they can be created
using the `MantaJobBuilder` class. `MantaJobBuilder` provides a fluent interface
that allows for an easier API for job creation and it provides a number of
useful functions for common use cases.

* [Jobs using MantaClient](/java-manta-examples/src/main/java/JobsWithMantaClient.java)
* [Jobs using MantaJobBuilder](/java-manta-examples/src/main/java/JobsWithMantaJobBuilder.java)

For more examples, check the included [integration test module](/java-manta-it).

### Server-side Multipart Upload

Manta supports multipart upload for dividing large files into many smaller parts 
and uploading them to Manta, where they can be assembled. The design for the 
server-side multipart upload is specified in 
[RFD 65](https://github.com/joyent/rfd/tree/master/rfd/0065). The Java SDK
implements an interface to make using server-side multipart uploading 
straightforward. The strategy to use for multipart upload is listed below.

1. Create a `MantaClient` instance.
2. Create an instance of `ServerSideMultipartManager` passing in the instance of
   MantaClient to the constructor.
3. Initiate an upload to the full path where the final object should be stored.
4. Upload each part using the `ServerSideMultipartUpload` object created in the
   previous step. The order that parts are uploaded does not matter,
   what does matter is that each part has the appropriate part number specified.
5. Execute `complete` to commit the parts to the object on the server. At this 
   point the server will assemble the final object.

An example application is provided in the **java-manta-examples** module, named
[ServerMultipart](/java-manta-examples/src/main/java/ServerMultipart.java),
to help illustrate the workflow.

### Encrypted Multipart Upload

When using client-side encryption with multipart upload there are some 
additional restrictions imposed on the implementor of the SDK:

* Uploaded parts must be uploaded in sequential order (e.g. part 1, 
  part 2, ...).
* Different parts can not be uploaded between different JVMs.
* At this time only the cipher mode AES/CTR is supported.

You will need to configure an instance of MantaClient using the
settings you would for client-side encryption, namely setting
`manta.client_encryption` to `true` and configuring the cipher and
encryption key.  Additionally, you need to create an instance of
`EncryptedServerSideMultipartManager`, which should be reused, for
uploading the various file parts and assembling the final file.

An example application is provided in the java-manta-examples module,
named [ClientEncryptionServerMultipart](/java-manta-examples/src/main/java/ClientEncryptionServerMultipart.java),
to help illustrate the workflow.

## Subuser Difficulties

If you are using subusers, be sure to specify the Manta account name as `user/subuser`.
Also, a common problem is that you haven't granted the subuser access to the
path within Manta. Typically this is done via the
[Manta CLI Tools](https://apidocs.joyent.com/manta/commands-reference.html)
using the [`mchmod` command](https://github.com/joyent/node-manta/blob/master/docs/man/mchmod.md).
This can also be done by adding roles on the `MantaHttpHeaders` object.

For example:

```bash
mchmod +subusername /user/stor/my_directory
```

## Contributions

Contributions welcome! Please read the [CONTRIBUTING.md](/docs/CONTRIBUTING.md) document for details
on getting started.

### Testing

Please refer to the [testing documentation](/docs/TESTING.md).

### Releasing

Please refer to the [release documentation](/docs/RELEASING.md).

### Bugs

See <https://github.com/joyent/java-manta/issues>.

## License
Java Manta is licensed under the MPLv2. Please see the [LICENSE.txt](/LICENSE.txt)
file for more details. The license was changed from the MIT license to the MPLv2
license starting at version 2.3.0.

### Credits
We are grateful for the functionality provided by the libraries that this project
depends on. Without them, we would be building everything from scratch. A thank you
goes out to:

* [The Apache Commons Project](https://commons.apache.org/)
* [The Apache HTTP Components Project](http://hc.apache.org/)
* [The FastXML Project](https://github.com/FasterXML)
* [The Legion of the Bouncy Castle Project](https://www.bouncycastle.org/)
* [The SLF4J Project](http://www.slf4j.org/)
* [The JNAGMP Project](https://github.com/square/jna-gmp)
* [The TestNG Project](http://testng.org/doc/index.html)
* [The Mockito Project](http://site.mockito.org/)
* [Timothy W Macinta's FastMD5 Project](http://twmacinta.com/myjava/fast_md5.php)
