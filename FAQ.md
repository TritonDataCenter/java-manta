# FAQ

## What projects are using the Java Manta SDK?

Some open source projects built on the SDK are listed [in the README](/README.md).

## How else can I interact with Manta?

The [node-manta project](https://github.com/joyent/node-manta) provides access to Manta
for JavaScript runtimes and offers a range of CLI commands like `mget` and `mput` which
make it easy to interact with Manta.

## How do I run the test suite?

`mvn test` will run unit tests while `mvn verify` will include integration tests and benchmarks.

See the [installation documentation](/USAGE.md) for setting up your environment and the
[testing documentation](/TESTING.md) for additional notes about running the test suite.

## I'm having issues with logging configuration, why don't logs show up?

Some logging frameworks are affected by shading and your configuration will need to account for
the modified package paths. See the [usage notes on logging](/USAGE.md#logging)
for more info.

## I'm experiencing issues with subusers, what's wrong?

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

## Why does Maven report duplicate classes when combining `java-manta-client-unshaded` with `java-manta-client-kryo-serialization`?

In order to provide compatibility between the client module and `java-manta-client-kryo-serialization`
we've elected to structure our dependencies such that the serialization module depends on the shaded client
module. This results in duplicate classes being encountered when the shaded client module is used in combination
with the serialization module. This can be resolved by adding an `<exclusion>` to the serialization module
dependency, e.g.:

```xml
    <dependency>
        <groupId>com.joyent.manta</groupId>
        <artifactId>java-manta-client-unshaded</artifactId>
        <version>x.y.z</version>
    </dependency>

    <dependency>
        <groupId>com.joyent.manta</groupId>
        <artifactId>java-manta-client-kryo-serialization</artifactId>
        <version>x.y.z</version>
        <exclusions>
            <exclusion>
                <groupId>com.joyent.manta</groupId>
                <artifactId>java-manta-client</artifactId>
            </exclusion>
        </exclusions>
    </dependency>
```

## Does Manta support multipart uploads?

Newer versions of Manta support multipart upload for dividing large files into many smaller parts
which are recombined server-side. The design for the server-side multipart upload is specified in
[RFD 65](https://github.com/joyent/rfd/tree/master/rfd/0065). The Java SDK
implements an interface to make using server-side multipart uploading
straightforward. The strategy to use for multipart upload is listed below.

1. Create a `MantaClient`.
2. Create a `ServerSideMultipartManager`.
3. Initiate an upload to the full path where the final object should be stored.
4. Upload each part using the `ServerSideMultipartUpload` object created in the
   previous step. The order that parts are uploaded does not matter,
   what does matter is that each part has the appropriate part number specified.
5. Execute `complete` to commit the parts to the object on the server. At this
   point the server will assemble the final object.

An example application is provided in the **java-manta-examples** module, named
[ServerMultipart](/java-manta-examples/src/main/java/ServerMultipart.java),
to help illustrate the workflow.

## How does client-side encryption interact with multipart uploads?

When using client-side encryption with multipart upload there are some
additional restrictions imposed on the implementor of the SDK:

* Uploaded parts must be uploaded in sequential order (e.g. part 1,
  part 2, ...).
* Different parts can not be uploaded between different JVMs.
* At this time only the cipher mode AES/CTR is supported.

You will need to configure a `MantaClient` using the
settings you would for client-side encryption, namely setting
`manta.client_encryption` to `true` and configuring the cipher and
encryption key.  Additionally, you need to create a
`EncryptedServerSideMultipartManager`, which should be reused, for
uploading the various file parts and assembling the final file.

An example application is provided in the java-manta-examples module,
named [ClientEncryptionServerMultipart](/java-manta-examples/src/main/java/ClientEncryptionServerMultipart.java),
to help illustrate the workflow.

## How do Accounts/Usernames/Subusers differ?

Joyent's SmartDataCenter [Role-based Access Control (RBAC)
implementation](https://docs.joyent.com/public-cloud/rbac/users)
defines the following categories:

 - Account: The group created for your company on Triton, managed by
 a user (the account owner) and zero or more subusers.
 - User: Users are login credentials that allow different people in your
 organization to log in to your Joyent Cloud account. Generally, users
 are account owners, but this term may be used for account owners and
 in place of "subusers".
 - Subuser: A subuser belongs to the account holder, who maintains control
 of the subusers. This may be people in your organizations who belong to the
 larger company account. Subusers have a unique username that is joined with
 the account holder's username. This is in the format of "user/subuser".

Within the Java Manta library, we refer to the identity used to login -
"owner" or "owner/subuser" - as *user*, e.g `MANTA_USER` or `manta.user`.

## How can I set up the client to connect to a personal Manta deployment?

If your Manta deployment has a self-signed certificate and no public DNS set up,
some extra work must be done to connect using the `java-manta` client.

1. Retrieve the self-signed certificate from your Manta deployment:
```
headnode$ sdc-sapi /services?name=loadbalancer | \
    json -Ha metadata.SSL_CERTIFICATE
```

2. Paste *only the certificate* (The lines between and including
`-----BEGIN CERTIFICATE-----` and `-----END CERTIFICATE-----`) into a file on
the machine from which you want to run the java-manta client - for example,
`/tmp/manta-ssl.crt`

3. Get the certificate's CN:
`openssl x509 -text -noout -in /tmp/manta-ssl.crt | grep CN`

4. Retrieve your Manta deployment's public IP address - this is the public IP
of the `loadbalancer` zone and can be found by running `ifconfig` from that
zone.

5. On the machine from which you want to run the `java-manta` client, edit
`/etc/hosts` to add an entry mapping the Manta IP address to the Manta CN - for
example:
```
10.99.99.5 manta.virtual.example.com
```

6. Add the certificate to your Java installation's keystore - this is often
found at `$JAVA_HOME/lib/security/cacerts` but can be customized. To add the
certificate, run:

```
$JAVA_HOME/bin/keytool -import -alias "<ALIAS OF YOUR CHOICE>" \
-keystore $JAVA_HOME/lib/security/cacerts -file "/tmp/manta-ssl.crt"
```

7. Set `MANTA_URL` to be the CN you entered in `/etc/hosts`, and set
`MANTA_TLS_INSECURE=true` to allow for the self-signed certificate. You may also
set these values as Java system properties rather than environment variables.
You should now be able to connect to your Manta deployment from the `java-manta`
client.
