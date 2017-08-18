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
* [Java Cryptography Extension](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) to
  use Client-side Encryption features.

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
$ mvn package
```

Which will compile the jar to ./targets/java-manta-version.jar. You can then
add it as a dependency to your Java project.

If you want to skip running of the test suite, use the `-DskipTests` property.

## Configuration

Configuration can be done through both system properties or environment variables, with the environment taking
precedence over system properties. Listed below are the minimum configuration options you'll need to get started.

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
The fingerprint for the public key used to access the manta service. Can be retrieved using `ssh-keygen -l -f ${MANTA_KEY_PATH} -E md5 | cut -d' ' -f 2`
* `manta.key_path` ( **MANTA_KEY_PATH**)
The name of the file that will be loaded for the account used to access the manta service.

Please refer to the [configuration documentation](/INSTALL.md) for the full
list of configuration options, which include retry and performance tuning in addition to encryption and authentication
parameters.

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

For more examples, check the included [examples module](/java-manta-examples) and the
[integration test module](/java-manta-it/src/test/java/com/joyent/manta/client).

## Contributions

Contributions are welcome! Please read the [CONTRIBUTING.md](/docs/CONTRIBUTING.md) document for details
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
