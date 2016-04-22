[![Build Status](https://travis-ci.org/joyent/java-manta.svg?branch=travis)](https://travis-ci.org/joyent/java-manta)

# Java Manta Client SDK

[java-manta](http://joyent.github.com/java-manta) is a community-maintained Java
SDK for interacting with Joyent's Manta system.

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
    <version>LATEST</version>
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

## Configuration

Configuration parameters take precedence from left to right - values on the
left are overridden by values on the right.

| Default                              | TestNG Param         | System Property           | Environment Variable      |
|--------------------------------------|----------------------|---------------------------|---------------------------|
| https://us-east.manta.joyent.com:443 | manta.url            | manta.url                 | MANTA_URL                 |
|                                      | manta.user           | manta.user                | MANTA_USER                |
|                                      | manta.key_id         | manta.key_id              | MANTA_KEY_ID              |
| $HOME/.ssh/id_rsa                    | manta.key_path       | manta.key_path            | MANTA_KEY_PATH            |
|                                      |                      | manta.key_content         | MANTA_KEY_CONTENT         |
|                                      |                      | manta.password            | MANTA_PASSWORD            |
| 20000                                | manta.timeout        | manta.timeout             | MANTA_TIMEOUT             |
| 3 (6 for integration tests)          |                      | manta.retries             | MANTA_HTTP_RETRIES        |
| 24                                   |                      | manta.max_connections     | MANTA_MAX_CONNS           |
| ApacheHttpTransport                  | manta.http_transport | manta.http_transport      | MANTA_HTTP_TRANSPORT      |
| TLSv1.2                              |                      | https.protocols           | MANTA_HTTPS_PROTOCOLS     |
| <value too big - see code>           |                      | https.cipherSuites        | MANTA_HTTPS_CIPHERS       |
| false                                |                      | manta.no_auth             | MANTA_NO_AUTH             |
| false                                |                      | manta.disable_native_sigs | MANTA_NO_NATIVE_SIGS      |
| 0                                    |                      | http.signature.cache.ttl  | MANTA_SIGS_CACHE_TTL      |

* `manta.url` ( **MANTA_URL** )
The URL of the manta service endpoint to test against
* `manta.user` ( **MANTA_USER** )
The account name used to access the manta service. If accessing via a [subuser](https://docs.joyent.com/public-cloud/rbac/users),
you will specify the account name as "user/subuser".
* `manta.key_id`: ( **MANTA_KEY_ID**)
The fingerprint for the public key used to access the manta service.
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
* `manta.http_transport` (**MANTA_HTTP_TRANSPORT**)
The HTTP transport library to use. Either the Apache HTTP Client (ApacheHttpTransport) or the native JDK HTTP library (NetHttpTransport).
* `https.protocols` (**MANTA_HTTPS_PROTOCOLS**)
A comma delimited list of TLS protocols.
* `https.cipherSuites` (**MANTA_HTTPS_CIPHERS**)
A comma delimited list of TLS cipher suites.
* `manta.no_auth` (**MANTA_NO_AUTH**)
When set to true, this disables HTTP Signature authentication entirely. This is
only really useful when you are running the library as part of a Manta job.
* `http.signature.native.rsa` (**MANTA_NO_NATIVE_SIGS**)
When set to true, this disables the use of native code libraries for cryptography.
* `http.signature.cache.ttl` (**MANTA_SIGS_CACHE_TTL**)
Time in milliseconds to cache the HTTP signature authorization header. A setting of
0ms disables the cache entirely.

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

## Test Suite
By default, the test suite invokes the java manta client against the live manta
service.  In order to run the test suite, you will need to specify environment
variables, system properties or TestNG parameters to tell the library how to
authenticate against Manta.

### Example Get Request
``` java
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

public class App {

    public static void main(String... args) throws IOException {
        ConfigContext config = new StandardConfigContext()
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser("user/subuser")
                .setMantaKeyPath("src/test/java/data/id_rsa")
                .setMantaKeyId("04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df");
        MantaClient client = new MantaClient(config);

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

### Example Job Execution

Jobs can be created directly with the `MantaClient` class or they can be created
using the `MantaJobBuilder` class. `MantaJobBuilder` provides a fluent interface
that allows for an easier API for job creation and it provides a number of
useful functions for common use cases.

#### Using MantaClient

Creating a job using the `MantaClient` API is done by making a number of calls
against the API and passing the job id to each API call. Here is an example that
processes 4 input files, greps them for 'foo' and returns the unique values.

``` java
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaJob;
import com.joyent.manta.client.MantaJobPhase;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

public class App {
    public static void main(String... args) throws IOException, InterruptedException {
        ConfigContext config = new StandardConfigContext()
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser("user/subuser")
                .setMantaKeyPath("src/test/java/data/id_rsa")
                .setMantaKeyId("04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df");
        MantaClient client = new MantaClient(config);

        List<String> inputs = new ArrayList<>();
        // You will need to change these to reflect the files that you want
        // to process
        inputs.add("/user/stor/logs/input_1");
        inputs.add("/user/stor/logs/input_2");
        inputs.add("/user/stor/logs/input_3");
        inputs.add("/user/stor/logs/input_4");

        List<MantaJobPhase> phases = new ArrayList<>();
        // This creates a map step that greps for 'foo' in all of the inputs
        MantaJobPhase map = new MantaJobPhase()
                .setType("map")
                .setExec("grep foo");
        // This returns unique values from all of the map outputs
        MantaJobPhase reduce = new MantaJobPhase()
                .setType("reduce")
                .setExec("sort | uniq");
        phases.add(map);
        phases.add(reduce);

        // This builds a job
        MantaJob job = new MantaJob("example", phases);
        UUID jobId = client.createJob(job);

        // This attaches the input data to the job
        client.addJobInputs(jobId, inputs.iterator());

        // This runs the job
        client.endJobInput(jobId);

        // This will get the status of the job
        MantaJob runningJob = client.getJob(jobId);

        // Wait until job finishes
        while (!client.getJob(jobId).getState().equals("done")) {
            Thread.sleep(3000L);
        }

        // Grab the results of the job as a string - in this case, there will
        // be only a single output
        // You will always need to close streams because we do everything online
        try (Stream<String> outputs = client.getJobOutputsAsStrings(jobId)) {
            // Print each output
            outputs.forEach(o -> System.out.println(o));
        }
    }
}
```

#### Using MantaJobBuilder

Creating a job using the `MantaJobBuilder` allows for a more fluent style of
job creation. Using this approach allows for a more fluent configuration of
job initialization.

```java
import com.joyent.manta.client.MantaClient;
import com.joyent.manta.client.MantaJobBuilder;
import com.joyent.manta.client.MantaJobPhase;
import com.joyent.manta.config.ConfigContext;
import com.joyent.manta.config.StandardConfigContext;

import java.io.IOException;
import java.util.stream.Stream;

public class App {
    public static void main(String... args) throws IOException, InterruptedException {
        ConfigContext config = new StandardConfigContext()
                .setMantaURL("https://us-east.manta.joyent.com")
                // If there is no subuser, then just use the account name
                .setMantaUser("user/subuser")
                .setMantaKeyPath("src/test/java/data/id_rsa")
                .setMantaKeyId("04:92:7b:23:bc:08:4f:d7:3b:5a:38:9e:4a:17:2e:df");
        MantaClient client = new MantaClient(config);

        // You can only get a builder from a MantaClient
        final MantaJobBuilder builder = client.jobBuilder();

        MantaJobBuilder.Run runningJob = builder.newJob("example")
                .addInputs("/user/stor/logs/input_1",
                        "/user/stor/logs/input_2",
                        "/user/stor/logs/input_3",
                        "/user/stor/logs/input_4")
                .addPhase(new MantaJobPhase()
                        .setType("map")
                        .setExec("grep foo"))
                .addPhase(new MantaJobPhase()
                        .setType("reduce")
                        .setExec("sort | uniq"))
                // This is an optional command that will validate that the inputs
                // specified are available
                .validateInputs()
                .run();

        // This will wait until the job is finished
        MantaJobBuilder.Done finishedJob = runningJob.waitUntilDone()
                // This will validate if the job finished without errors.
                // If there was an error an exception will be thrown
                .validateJobsSucceeded();

        // You will always need to close streams because we do everything online
        try (Stream<String> outputs = finishedJob.outputs()) {
            // Print each output
            outputs.forEach(o -> System.out.println(o));
        }
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

Contributions welcome! Please read the [CONTRIBUTIONS.MD](CONTRIBUTIONS.MD) document for details
on getting started.

### Testing

When running the unit tests, you will need an active account on the Joyent public
cloud or a private Manta instance. To test:
```
# Assuming you have already set your environment variables and/or system properties
mvn test
```

### Releasing

Please refer to the [release documentation](RELEASING.md).

### Bugs

See <https://github.com/joyent/java-manta/issues>.

## License
Java Manta is licensed under the MPLv2. Please see the [LICENSE.txt](LICENSE.txt) 
file for more details. The license was changed from the MIT license to the MPLv2
license starting at version 2.3.0.
