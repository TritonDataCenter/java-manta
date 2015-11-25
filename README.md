[![Build Status](https://travis-ci.org/joyent/java-manta.svg?branch=travis)](https://travis-ci.org/joyent/java-manta)

# Java Manta Client SDK

[java-manta](http://joyent.github.com/java-manta) is a community-maintained Java
SDK for interacting with Joyent's Manta system.

At present, this SDK only supports the Manta data plane, and not the Manta
compute component.

## Installation 

### Requirements
* [Java 1.8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) or higher.
* [Maven 3.3.x](https://maven.apache.org/)

### Using Maven
Add the latest java-manta dependency to your Maven `pom.xml`.

```xml
<dependency>
    <groupId>com.joyent.manta</groupId>
    <artifactId>java-manta</artifactId>
    <version>2.0.0</version>
</dependency>
```

### From Source
If you prefer to build from source, you'll also need
[Maven](https://maven.apache.org/), and then invoke:

``` bash
# mvn package
```

Which will compile the jar to ./targets/java-manta-version.jar. You can then
add it as a dependency to your Java project.

## Test Suite
By default, the test suite invokes the java manta client against the live manta
service.  In order to run the test suite, you will need to specify environment
variables, system properties or TestNG parameters to tell the library how to
authenticate against Manta.

Configuration parameters take precedence from left to right - values on the
left are overridden by values on the right.

| Default                          | TestNG Param   | System Property | Environment Variable |
|----------------------------------|----------------|-----------------|----------------------|
| https://us-east.manta.joyent.com | manta.url      | manta.url       | MANTA_URL            |
|                                  | manta.user     | manta.user      | MANTA_USER           |
|                                  | manta.key_id   | manta.key_id    | MANTA_KEY_ID         |
|                                  | manta.key_path | manta.key_path  | MANTA_KEY_PATH       |
| 20000                            | manta.timeout  | manta.timeout   | MANTA_TIMEOUT        | 

* `manta.url` ( **MANTA_URL** )
The URL of the manta service endpoint to test against
* `manta.user` ( **MANTA_USER** )
The account name used to access the manta service. If accessing via a [subuser](https://docs.joyent.com/public-cloud/rbac/users),
you will specify the account name as "user/subuser".
* `manta.key_id`: ( **MANTA_KEY_ID**)
The fingerprint for the public key used to access the manta service.
* `manta.key_path` ( **MANTA_KEY_PATH** )
The name of the file that will be loaded for the account used to access the manta service.
* `manta.timeout` ( **MANTA_TIMEOUT**)
The number of milliseconds to wait after a request was made to Manta before failing.
 
If you want to skip running of the test suite, use the `-DskipTests` property.

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

You'll need a manta login, an associated rsa key, and its corresponding key
fingerprint. Note that this api currently only supports rsa ssh keys --
enterprising individuals wishing to use dsa keys can contribute to this repo by
consulting the [node-http-signing
spec](https://github.com/joyent/node-http-signature/blob/master/http_signing.md).

For detailed usage instructions, consult the provided javadoc.

### Example Get Request
``` java
import com.joyent.manta.client.MantaClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class App {
    private static final String URL = "https://us-east.manta.joyent.com";
    // If there is no subuser, then just use the account name
    private static final String LOGIN = "user/subuser";
    private static final String KEY_PATH = "src/test/java/data/id_rsa";
    private static final String KEY_FINGERPRINT = "04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df";

    public static void main(String... args) throws IOException {
        MantaClient client = new MantaClient(URL, LOGIN, KEY_PATH, KEY_FINGERPRINT);

        String mantaFile = "/user/stor/foo";

        // Print out every line from file streamed real-time from Manta
        try (InputStream is = client.getAsInputStream(mantaFile);
             Scanner scanner = new Scanner(is)) {

            while (scanner.hasNextLine()) {
                System.out.println(scanner.nextLine());
            }
        }

        // Load file into memory as a string directly from Manta
        String data = client.getAsString(mantaFile);
        System.out.println(data);
    }
}
```

For more examples, check the included integration tests.

### Logging

The SDK utilizes [slf4j](http://www.slf4j.org/), and logging
can be configured using a SLF4J implementation. The underlying
[google-http-java-client](https://code.google.com/p/google-http-java-client/)
utilizes
[java.util.logging.Logger](http://docs.oracle.com/javase/7/docs/api/java/util/logging/Logger.html),
which can be configured
[accordingly](https://code.google.com/p/google-http-java-client/wiki/HTTP).

## Subuser Difficulties

If you are using subusers, be sure to specify the Manta account name as `user/subuser`.
Also, a common problem is that you haven't granted the subuser access to the
path within Manta. Typically this is done via the [Manta CLI Tools](https://apidocs.joyent.com/manta/commands-reference.html)
using the [`mchmod` command](https://github.com/joyent/node-manta/blob/master/docs/man/mchmod.md).
This can also be done by adding roles on the `MantaHttpHeaders` object.

For example:

```bash
mchmod +subusername /user/stor/my_directory
```

## Contributions

Contributions welcome! Please ensure that `# mvn checkstyle:checkstyle -Dcheckstyle.skip=false` runs
clean with no warnings or errors.

### Testing

When running the unit tests, you will need an active account on the Joyent public 
cloud or a private Manta instance. To test:
```
# Assuming you have already set your environment variables and/or system properties
mvn test
```

### Releasing

In order to release to [Maven central](https://search.maven.org/), you will need [an account] (https://issues.sonatype.org) with [Sonatype OSSRH](http://central.sonatype.org/pages/ossrh-guide.html).
If you do not already have an account, you can click the signup link from the login screen
to begin the process of registering for an account.  After signing up, you will need to add
your sonatype credentials to your your maven settings file.  By default this settings file is
located at `$HOME/.m2/settings.xml`.  In addition to sonatype credentials, you will
also need to add a [gpg signing](https://maven.apache.org/plugins/maven-gpg-plugin/sign-mojo.html) key configuration.

For the security conscious, a [guide to encrypting credentials in maven settings files](https://maven.apache.org/guides/mini/guide-encryption.html) exists to
illustrate how credentials can be protected.

The following is an example settings.xml file:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                              http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <profiles>
    <profile>
      <id>gpg</id>
      <properties>
        <!-- Customize the following properties to configure your gpg settings. -->
        <gpg.executable>gpg</gpg.executable>
        <gpg.keyname>keyname</gpg.keyname>
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

1. Make sure the source builds, test suites pass, and the source and java artifacts can
 be generated and signed:  
`mvn clean verify -Prelease`
2. Start from a clean working directory and make sure you have no modified
files in your workspace:  
`mvn clean && git status`
3. Prepare the release:  
4. `mvn release:clean release:prepare`
4. Enter the version to be associated with this release.
You should be prompted for this version number, and the default assumed version
will be shown and should correspond to the version that was in the pom.xml
file but WITHOUT the `-SNAPSHOT` suffix.
5. Enter the SCM tag to be used to mark this commit in the SCM.
You should be prompted for this tag name, and the default will be
`{projectName}-{releaseVersion}`
6. Enter the new development version.
You should be prompted for this version number, and the default for this will
be an incremented version number of the release followed by a `-SNAPSHOT`
suffix.

 At this point
 * The release plugin will continue and build/package the artifacts.
 * The pom.xml file will be updated to reflect the version change to the release
version.
 * The new pom.xml will be committed.
 * The new commit will be tagged.     
 * The pom.xml file will be updated again with the new development version.
 * Then this new pom.xml will be committed.

 If the process fails for some reason during any of these points, you can invoke
`mvn release:rollback` to go back to the preparation point and try again, but
you will also have to revert any SCM commits that were done
(`git reset --hard HEAD^1` command works well for this) as well as remove any
tags that were created (`git tag -l && git tag -d <tagName>` commands help
with this).

7. Push tags to github:
`git push --follow-tags`
In order for the `release:perform` goal to complete successfully, you will need to
push the tags created by the maven release plugin to the remote git server.

8. Perform the actual release:  
`mvn release:perform`
A build will be performed and packaged and artifacts deployed to the sonatype
staging repository.

9. Log into the [Sonatype OSSHR Next](https://oss.sonatype.org) web interface
to [verify and promote](http://central.sonatype.org/pages/releasing-the-deployment.html)
the build. 

**NOTE**: By default, these instructions assumes the release is being done from a
branch that can be merged into a primary branch upon successful completion,
and that the SCM operations that are carried out by maven plugins will NOT
access the repo, but rather, work on a local copy instead.  The release plugin
as configured in the maven repo sets values for this assumption
(`localCheckout=true` and `pushChanges=false`).

**NOTE**: If the release is being done in a separate fork of the primary
github repo, doing a merge via pull request will not also copy the tags that
were created during the release process.  The tags will have to be created in
the primary repo separately, but this may be preferred anyway.

### Bugs

See <https://github.com/joyent/java-manta/issues>.

## License

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

