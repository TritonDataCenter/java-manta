# Java Manta Client SDK

[java-manta](http://joyent.github.com/java-manta) is a community-maintained Java
SDK for interacting with Joyent's Manta system.

At present, this SDK only supports the Manta data plane, and not the Manta
compute component.

# Installation
## Requirements.
* [Java 1.7](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher.
* [Maven](https://maven.apache.org/)

## Using Maven
Add the latest java-manta dependency to your Maven `pom.xml`.

```xml
<dependency>
    <groupId>com.joyent.manta</groupId>
    <artifactId>java-manta</artifactId>
    <version>1.5.4</version>
</dependency>
```

## From Source
If you prefer to build from source, you'll also need
[Maven](https://maven.apache.org/), and then invoke:

``` bash
# mvn package
```

Which will compile the jar to ./targets/java-manta-version.jar. You can then
add it as a dependency to your Java project.

### Test Suite:
By default, the test suite invokes the java manta client against the live manta
service.  In order to run the test suite, the account name used to access the
manta service must be specified.  This can be done by setting the MANTA_USER
environment variable to the account name or specifying the account name as a
TestNG configuration parameter named `manta.accountName` in the test suite
configuration parameters located in `./src/test/resources/testng.xml`.
The public key for testing located at `./src/test/resources/manta-test_rsa.pub`
must also be installed into the joyent account that will access the manta
service.  The `testng.xml` file has other parameters that can be changed to point
to a desired manta endpoint, as well as change the keys used to authenticate
against the manta service.

The test suite configuration file defines properties used to configure the manta client for testing.
Each of these properties can be overridden with corresponding environment variables.

* `manta.url` ( **MANTA_URL** )
The URL of the manta service endpoint to test against
* `manta.accountName` ( **MANTA_USER** )
The account name used to access the manta service
* `manta.test.key.fingerprint`: ( **MANTA_KEY_ID**)
The fingerprint for the public key used to access the manta service.
* `manta.test.key.private.filename` ( **MANTA_KEY_PATH** )
The name of the file that will be searched in the classpath containing the private key content
for the account used to access the manta service.
 
If you want to skip running of the test suite, use the `-DskipTests` property.

# Usage

You'll need a manta login, an associated rsa key, and its corresponding key
fingerprint. Note that this api currently only supports rsa ssh keys --
enterprising individuals wishing to use dsa keys can contribute to this repo by
consulting the [node-http-signing
spec](https://github.com/joyent/node-http-signature/blob/master/http_signing.md).

For detailed usage instructions, consult the provided javadoc.

## Example Get Request
``` java
import java.io.IOException;

import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaObject;
import com.joyent.manta.client.MantaUtils;
import com.joyent.manta.exception.MantaCryptoException;

public class App {
        private static MantaClient CLIENT;
        private static final String URL = "https://us-east.manta.joyent.com";
        private static final String LOGIN = "yunong";
        private static final String KEY_PATH = "src/test/java/data/id_rsa";
        private static final String KEY_FINGERPRINT = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";

        public static void main(String... args) throws IOException, MantaCryptoException {
                CLIENT = MantaClient.newInstance(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);
                MantaObject gotObject = CLIENT.get("/yunong/stor/foo");
                String data = MantaUtils.inputStreamToString(gotObject.getDataInputStream());
                System.out.println(data);
        }
}
```

For more examples, check the included unit tests.

# Logging

The SDK utilizes [slf4j](http://www.slf4j.org/), and logging
can be configured using a SLF4J implementation. The underlying
[google-http-java-client](https://code.google.com/p/google-http-java-client/)
utilizes
[java.util.logging.Logger](http://docs.oracle.com/javase/7/docs/api/java/util/logging/Logger.html),
which can be configured
[accordingly](https://code.google.com/p/google-http-java-client/wiki/HTTP).

# Contributions

Contributions welcome! Please ensure that `# mvn checkstyle:checkstyle` runs
clean with no warnings or errors.

## Testing

When running the unit tests, you will need an active account on the Joyent public 
cloud or a private Manta instance. Please be sure that all of the environment
variables needed for the Manta CLI SDK are set (`MANTA_URL`, `MANTA_USER`,
`MANTA_KEY_ID`). In addition to those environment variables, you will also need to
set `MANTA_KEY_PATH` with the path to the private key associated with the
`MANTA_KEY_ID` value.

To test:
```
# Assuming you have already set your environment variables
mvn test
```

## Releasing

In order to release to Maven central, you will need an account and your gpg signing
keys registered to your account. Once that is setup be sure to have the following
settings in your `$HOME/.m2/settings.xml`:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>ossrh</id>
      <activation>
        <activeByDefault>true</activeByDefault>
      </activation>
      <properties>
        <!-- Change to executable name -->
        <gpg.executable>gpg</gpg.executable>
        <gpg.passphrase>passphrase</gpg.passphrase>
        <gpg.secretKeyring>${env.HOME}/.gnupg/secring.gpg</gpg.secretKeyring>
      </properties>
    </profile>
  </profiles>
  <servers>
    <server>
      <id>ossrh</id>
      <username>username</username>
      <password>password</password>
    </server>
  </servers>
</settings>
```

To perform a release:

```
mvn clean deploy -P release
```

Then go to the [OSS Sonatype site](https://oss.sonatype.org) to verify and release.

# License

The MIT License (MIT)
Copyright (c) 2013 Joyent

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

# Bugs

See <https://github.com/joyent/java-manta/issues>.

