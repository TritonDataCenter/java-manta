# FAQ

## How do I run the test suite?

`mvn test` will run unit tests while `mvn verify` will include integration tests and benchmarks.

See [the testing documentation](/TESTING.md) for additional notes about running the test suite.

## How do Accounts/Usernames/Subusers differ?

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

## Does Triton Object Storage (Manta) support Multipart Uploads?

Newer versions of Manta support multipart upload for dividing large files into many smaller parts
which are recombined server-side. The design for the server-side multipart upload is specified in
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

## How does Client-side Encryption interact with Multipart Uploads?

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
