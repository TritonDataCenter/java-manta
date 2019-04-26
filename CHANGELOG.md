# Change Log
All notable changes to this project will be documented in this file.
This project aims to adhere to [Semantic Versioning](http://semver.org/).

## [3.4.1]
### Added
 - Added `MANTA_TLS_INSECURE` to allow disabling TLS certificate verification in
   certain test environmnts.  Not intended to be used in porduction.
 - Updated `MantaErrorCode` Enum Values, mapping more errors from Manta components including muskie, mahi,
   moray, electric-moray, piranha and marlin. Deprecated errors with invalid rest codes and removed undefined 
   Manta Errors in the SDK.
 - [Developer's Guide](https://github.com/joyent/java-manta/pull/495) added for professionals within Joyent, Inc
   and the community interested in making open-source contributions.
 - Integration-Test `MantaClientSnapLinksIT` to verify behaviour of the SDK for user accounts that
   have `snaplinks` disabled.
 - More documentation in `MetricReporterSupplier` to provide information regarding how the SDK exposes client metrics
   to [manta-monitor](https://github.com/joyent/manta-monitor).
### Changed
 - Copyright Header updates for files that have been modified recently within the Client Java SDK.     
### Fixed
 - [Standardize .equals() and .hashCode() behaviour](https://github.com/joyent/java-manta/issues/477)
 - [Review correctness of integer division in floating point context](https://github.com/joyent/java-manta/issues/479)
 - [Integration tests structured better to support accounts having SnapLinks disabled](https://github.com/joyent/java-manta/issues/480)
 - [NoApiServersAvailable is not present in MantaErrorCode enums](https://github.com/joyent/java-manta/issues/470)
 - [Formatting Mbean attribute 999thPercentile for the metric object requests](https://github.com/joyent/java-manta/issues/436)
 
## [3.4.0] - 2019-03-22
### Added
 - Configuration parameter `manta.content_type_detection` is now added to allow for the user to explicitly disable
   automatic content type detection dependent on the data type being uploaded to Manta using the SDK. 
   It is updated to a default value of true (which aligns with the default behaviour of the SDK).    
 - [Updated Prune-Depth Documentation. Add Unit Tests for more limit values](https://github.com/joyent/java-manta/pull/484)
### Changed
 - [Upgraded dependencies to latest stable versions:](https://github.com/joyent/java-manta/commit/1ea4694b2747c42fa7b8b60733fbd7699a3f2426)
     - HTTP Signatures dependency: 4.0.9 → 4.0.10
     - BouncyCastle: 1.60 → 1.61
     - Apache HttpClient: 4.5.6 → 4.5.7
     - Extra-Enforcer-Rules 1.0-beta-6 → 1.1
     - Javac-ErrorProne 2.8.2 → 2.8.5
     - Error-Prone-Core 2.1.1 → 2.3.3
     - FasterXml-UUID 3.1.4 → 3.1.5
     - Jackson 2.9.4 → 2.9.8
     - Checkstyle 8.2 → 8.18
     - Mockito 2.16.0 → 2.24.5
     - TestNG 6.11 → 6.14.3
     - Commons-Collections 4.1 → 4.3
     - Commons-Lang 3.6 → 3.8.1
     - Commons-IO 2.5 → 2.6
     - Commons-Codec 1.10 → 1.12
     - Commons-Text 1.1 → 1.6
     - Kryo 4.0.1 → 4.0.2
     - Objenesis 2.6 →  3.0.1
     - Cloning 1.9.6 → 1.9.12
     - Picocli 1.0.1 → 3.9.5
     - Dropwizard-Metrics 4.0.2 → 4.0.5
     - JMH 1.19 → 1.21
     - Wiremock 2.18.0 → 2.21.0 
### Fixed
 - [Create configuration setting that allows for disabling content-type autodetection](https://github.com/joyent/java-manta/issues/488)
 - [Content-Type is not set to application/octet-stream when doing CSE](https://github.com/joyent/java-manta/issues/486) 
 - [Invalid MantaUser results in 500 Internal Server Error](https://github.com/joyent/java-manta/issues/123)
 - [Change all hard coded "/" values to MantaClient.SEPARATOR ](https://github.com/joyent/java-manta/issues/363)  

## [3.3.1] - 2019-02-05
### Added
 - [Allow for authentication to be disabled in CSE](https://github.com/joyent/java-manta/issues/465)
   Client side encryption HMAC authentication of ciphertext for CTR/CBC ciphers
   can now be in VerificationDisabled mode. The default authentication mode continues to be
   Mandatory.
### Changed
 - [Deprecate Jobs Multipart implementation and disable integration tests](https://github.com/joyent/java-manta/issues/459)
 - Upgraded Bouncy Castle, Apache HTTP Client, Jackson and Java HTTP Signatures dependencies
### Fixed
 - [Client-side encryption does not use libnss when doing simple PUT](https://github.com/joyent/java-manta/issues/449)
 - [Server-side MPU is hardcoded to create a directory prefix of only a single character](https://github.com/joyent/java-manta/issues/451)
 - [Server-side MPU returns a committed result but it is parsed as COMMITTING](https://github.com/joyent/java-manta/issues/452)
 - [InterruptedException could be thrown warnings have been resolved](https://github.com/joyent/java-manta/issues/322)
 - [Prune empty directory depth configuration validation is incorrect](https://github.com/joyent/java-manta/issues/457) 

## [3.3.0] - 2018-12-02
### Changed
 - Configuration parameter `manta.timeout` is now being used to specify
   connection timeouts and is updated to a default value of 4s. This parameter now accepts negative values (indicates
   the OS default setting).
 - Configuration parameter `manta.tcp_socket_timeout` is now being 
   used to specify timeouts when waiting for a tcp socket and is updated to a default value of 20s. This parameter now 
   accepts negative values (indicates the OS default setting).
 - Configuration parameter `manta.connection_request_timeout` now accepts 
   negative values (indicates the OS default setting).
### Fixed
 - [Race condition in MantaObjectOutputStream causing MantaIOException](https://github.com/joyent/java-manta/issues/439)
 - [Manta client unable to set new connection timeout #441](https://github.com/joyent/java-manta/issues/441)
 - [Untangle the different timeout settings from MANTA_TIMEOUT](https://github.com/joyent/java-manta/issues/109)

## [3.2.5] - 2018-10-04
### Fixed
 - [Delete prune keeps trying to delete recursively when directory is not empty](https://github.com/joyent/java-manta/issues/435)
### Changed
 - Improved documentation, code comments and code path for delete prune operations.

## [3.2.4] - 2018-08-13
### Added 
 - [Prune Empty Parent Directory](https://github.com/joyent/java-manta/issues/425)
 	Can now attempt to delete empty parent directories within a delete. See 
 	`manta.prune_empty_parent_depth`/`MANTA_PRUNE_EMPTY_PARENT_DEPTH` in the configuration documentation
 	for an explanation of how this can be enabled.

## [3.2.3] - 2018-08-03
### Fixed
 - [UnsupportedOperationException when getting a 0 byte file using `MantaClient.getAsInputStream`](https://github.com/joyent/java-manta/issues/408)
 - [AutoContinuingInputStream fails to handle fatal exceptions correctly and triggers a self-suppression error.](https://github.com/joyent/java-manta/pull/429)

### Added
 - Client metrics can now be enabled by selecting a reporting mode with the
   [`manta.metric_reporter.mode`/`MANTA_METRIC_REPORTER_MODE`](/USAGE.md#parameters) setting. [JMX and SLF4J are
   available](https://github.com/joyent/java-manta/issues/410#issuecomment-384751882), though others may be added in the future.
 - New metrics exposed:
    - [Meter](http://metrics.dropwizard.io/4.0.0/manual/core.html#meter) for
    [retries](https://github.com/joyent/java-manta/issues/410).
    - [Timers](http://metrics.dropwizard.io/4.0.0/manual/core.html#timer) per HTTP request method.
    - [Meters](http://metrics.dropwizard.io/4.0.0/manual/core.html#meter) per exception that occurs during requests.
 - [`MantaClient#delete(String, MantaHttpHeaders)`](https://github.com/joyent/java-manta/issues/427)
 - [Download auto-resume](https://github.com/joyent/java-manta/issues/411) has been added in the form of
    [`manta.download_continuations`/`MANTA_DOWNLOAD_CONTINUATIONS` configuration
    setting](https://github.com/joyent/java-manta/blob/master/USAGE.md#download-continuation).

### Changed
 - MBeans registered in JMX no longer use an incrementing integer and instead are created under
   [unique IDs for each client](https://user-images.githubusercontent.com/1973223/38774088-6a08861e-4014-11e8-8951-287ecd70032f.png).
 - JMX is no longer used to expose configuration and pool stats by default. To reenable JMX,
   set `manta.metric_reporter.mode`/`MANTA_METRIC_REPORTER_MODE` to `JMX`.
 - Upgraded upstream dependencies to latest stable versions:
    - HTTP Signatures dependency: 4.0.6 → 4.0.8
    - jna-gmp: 2.0.0 → 2.1.0
    - BouncyCastle: 1.58 → 1.59
    - Apache HttpClient: 4.5.3 → 4.5.5

## [3.2.2] - 2018-05-04
### Fixed
 - [Misleading log message is shown for retrying requests that are unretryable](https://github.com/joyent/java-manta/issues/388)
 - [`MantaClientHttpResponseException` does not provide a way to get the response headers](https://github.com/joyent/java-manta/issues/383)
 - [Underlying stream is not closed when we throw a `MantaClientEncryptionException`](https://github.com/joyent/java-manta/issues/391)
 - [Underlying stream is not closed by `MantaEncryptedObjectInputStream` for range requests with certain ciphers](https://github.com/joyent/java-manta/issues/398)
 - [Improved error messages for missing or invalid private keys.](https://github.com/joyent/java-manta/pull/415)
 - [Exceptions relating to object type expectations are too generic](https://github.com/joyent/java-manta/issues/403)
 - [Integration test regression](https://github.com/joyent/java-manta/pull/417)

## [3.2.1] - 2017-12-01
### Added
 - [Load balancer IP](https://github.com/joyent/java-manta/pull/378) has been added
   to response headers as `x-load-balancer`. The header will be injected into the response if it is not already present.
 - Azul Systems' [Zulu JDK](https://www.azul.com/products/zulu-and-zulu-enterprise/)
   has been [added to the Travis-CI test matrix](https://github.com/joyent/java-manta/pull/382).
 - [Recursive directory creation](https://github.com/joyent/java-manta/issues/371)
   can now attempt to skip a configurable number of levels in order to avoid redundant `PUT` requests. See
   `manta.skip_directory_depth` / `MANTA_SKIP_DIRECTORY_DEPTH` in the configuration documentation for an explanation
   of how the optimization can be enabled.
 - `Expect: 100-continue` header [can now be set](https://github.com/joyent/java-manta/issues/314)
   by configuring a timeout value for `manta.expect_continue_timeout` / `MANTA_EXPECT_CONTINUE_TIMEOUT`.
### Fixed
 - [Signing of URLs which need to be encoded](https://github.com/joyent/java-manta/issues/379) has been fixed.
 - Java 9 [compatibility improvements](https://github.com/joyent/java-manta/pull/382).
 - `MantaClient#listObjects` [potentially leaking connections](https://github.com/joyent/java-manta/pull/385).

## [3.2.0] - 2017-11-03
### Added
 - [Added method MantaClient.find() which allows for recursive directory listing](https://github.com/joyent/java-manta/issues/87).
 - Support for reading content-type and content-md5 header information from 
   directory listings
 - Added [connection request timeout](https://github.com/joyent/java-manta/issues/347)
   configuration parameter: `manta.connection_request_timeout` / `MANTA_CONNECTION_REQUEST_TIMEOUT`
 - `MantaClient` will now accept an externally-created `HttpClientBuilder` in order to
   allow for customization now available through standard configuration parameters. Users are expected to provide a
   `MantaConnectionFactoryConfigurator` containing their custom instance to the
   `MantaClient(ConfigContext, MantaConnectionFactoryConfigurator)` constructor. Constructor documentation
   explains the benefits and trade offs.
 - `MantaClient` now allows for modification of authentication parameters by
   [dynamically-reloading configuration state](https://github.com/joyent/java-manta/issues/311). `AuthAwareConfigContext` can
   be used to wrap the desired configuration and provides a method to update derived objects atomically.
### Fixed
 - Clarify version history of `MantaInputStreamEntity`
 - MPU parts which were missing an ETag in their response were
   [not treated as errors](https://github.com/joyent/java-manta/issues/305).
 - NullPointerException as a result of some configuration parameters
   [not being handled correctly unless explicitly set](https://github.com/joyent/java-manta/issues/247).
 - Setting `manta.retries`/`MANTA_HTTP_RETRIES` to 0 would print `Retry of failed requests is disabled` but
   leave the default Apache HttpClient [retry behavior](https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/fundamentals.html#d5e316).
 - [Content type is set for file object in directory listing when it isn't available](https://github.com/joyent/java-manta/issues/341)
 - [Fixes validation guard clauses that are not validating anything](https://github.com/joyent/java-manta/issues/346)
 - [MDC logging of the load balancer address now logs the proper address](https://github.com/joyent/java-manta/issues/266)
 - [Fixes deleteRecursive fails with ConnectionPoolTimeoutException](https://github.com/joyent/java-manta/issues/358)
### Changed
 - Validation of paths passed to `MantaClient` is now more consistently strict.
   More useful errors should be thrown sooner for invalid paths, without any
   change to valid requests.
 - Core library code has has been extracted from `java-manta-client` into a separate module named
   `java-manta-client-unshaded` allowing users to incorporate the library into their project without bundled dependencies.
 - Constructor signatures for `StandardHttpHelper`, `EncryptionHttpHelper`, and `MantaSeekableByteChannel`
   which accepted a `MantaConnectionFactory` have been deprecated.

## [3.1.6] - 2017-08-15
### Fixed
 - Potential file corruption caused by automatic retries as a result of
   [503 responses](https://github.com/joyent/java-manta/issues/295)
   when utilizing client-side encryption with regular PUT requests.
 - MPU finalization meant it was impossible to retry last part in case of
   [503 responses](https://github.com/joyent/java-manta/issues/297).
 - Object content verification of standard PUT requests was being
   skipped if the server [omitted the computed MD5](https://github.com/joyent/java-manta/issues/298)
   from the response.

## [3.1.5] - 2017-07-28
### Fixed
 - MPU retries still [causing file corruption](https://github.com/joyent/java-manta/issues/290)
   when server responds with 503 Service Unavailable. Apache HttpClient code path audited to ensure
   no other automatic retries can occur.

## [3.1.4] - 2017-07-21
### Changed
 - The heuristics for guessing Content-Type have been adjusted to give
   [more consistent](https://github.com/joyent/java-manta/issues/276)
   results across platforms.
### Fixed
 - When using encryption in combination with Multipart Uploads,
   [automatic retries](https://github.com/joyent/java-manta/issues/284) triggered by the underlying HTTP
   library could cause file corruption. In case of a network error automatically certain requests would be retried
   transparently (e.g. those backed by `File`s and `byte[]` data). This caused authentication and encryption state
   erroneously include the partial content from the initial request. As a result of this fix encrypted MPU operations
   will utilize the BouncyCastle cryptography library even when libnss (PKCS#11) is installed.

## [3.1.3] - 2017-06-29
### Fixed
 - [`Digest` not reset in DigestedEntity](https://github.com/joyent/java-manta/pull/280)
   leading to `MantaChecksumFailedException` when the entity is reused during automatic retry.
 - [MantaChecksumFailedException](https://github.com/joyent/java-manta/pull/275)
   lacking exception context.

## [3.1.2] - 2017-06-22
### Fixed
 - [`InputStream` left open in EncryptingEntity and EncryptingPartEntity](https://github.com/joyent/java-manta/pull/264)
   leading to space from deleted files not being reclaimed until JVM shutdown.

## [3.1.1] - 2017-06-14
### Changed
 - Added additional error context information for IOExceptions thrown
   during read() in encrypted streams.
### Fixed
 - [MantaClient.move() does not support moving a file to a non-existent directory](https://github.com/joyent/java-manta/issues/256)
   This fix optionally allows users to recursively create a destination directory 
   structure on move(). This is useful for supporting third-party APIs/libraries
   that use this pattern due to a S3 first design. 
 - Resolve issue where
   [MantaClient.getAsInputStream().close() throws MantaIOException](https://github.com/joyent/java-manta/issues/250)
   for encrypted objects with trailing HMAC. Infrequently an issue could occur where
   less bytes than requested were read and the stream was left in a bad state.
   This would prevent the underlying connection from being returned to the connection pool.
 - Exception contexts now [include SDK version.](https://github.com/joyent/java-manta/issues/254)

## [3.1.0] - 2017-06-07

Several related URL encoding bugs have been fixed.  Objects with
non-alphanumeric characters created by java-manta may have been
created with unexpected encoding characters.  Only the object names
were affected, not the content.

### Changed
 - Paths with URL unsafe characters are now encoded correctly.
   Previously the
   [space character was being transformed](https://github.com/joyent/java-manta/issues/229)
   into a plus (`+`) character.  So a PUT to the Manta object
   `/user/stor/Hello World.txt`, would instead create
   `/user/stor/Hello+World.txt`.
 - All `MantaClient` operations now
   [consistently encode](https://github.com/joyent/java-manta/issues/230),
   and `MantaObject.getPath` always returns the original (not encoded)
   path.
 - Recursive directory creation will no longer
   [encode the path twice](https://github.com/joyent/java-manta/issues/231).
 - Gracefully handle missing JCE Unlimited Strength Policy by [only enabling stronger ciphers when
   they are allowed by the runtime](https://github.com/joyent/java-manta/issues/242).
   Installing the Unlimited Policy files where necessary is still strongly recommended.

## [3.0.0] - 2017-04-06
### Changed
 - Upgraded HTTP Signatures library to 4.0.1.
 - Moved configuration validation into a static method on ConfigContext.
 - Manta specific content-types are now contained in `MantaContentTypes`.
 - MantaClient.close() no longer throws Exception.
 - Liberalized boolean parsing in configuration - true, t, T, yes, and 1 are equal true.
 - Job related classes have been moved to the com.joyent.manta.client.jobs package.
 - In `BaseChainedConfigContext`, `setHttpsCiphers` has been renamed to 
   `setHttpsCipherSuites` to be in alignment with the getter of a similar name.
 - Request IDs are now generated using time-based UUIDs. 
### Added
 - Added version information to user agent and debug output. 
 - Added simple CLI to use for debugging.
 - Added configuration dump system property: manta.configDump
 - [Add JMX to monitor configuration and thread pools](https://github.com/joyent/java-manta/issues/133)
 - SettableConfigContext has been added as an interface to indicate that a given
   config context has settable properties.
 - Added isClosed() method to MantaClient.
 - Added HTTP buffer size configuration parameter.
 - Added TCP socket time out configuration parameter.
 - Added verify upload checksum configuration parameter.
 - Added upload buffer size configuration parameter.
 - Added getInputStream method that accepts byte range parameter.
 - Added support for client-side encryption.
 - Added support for server supported multipart.
 - Added support for client-side encrypted multipart upload.
 - Added support for client-side encrypted HTTP range download.
 - Added module supporting serializing encrypted MPU upload objects.
 - Added support for libnss via PKCS11.
 - Added support for FastMD5 native MD5 calculation.
 - Added support for single-second caching of HTTP signatures.
 - Added support for ECDSA and DSA HTTP signatures.
### Removed
 - Remove Google HTTP Client and replaced it with stand-alone Apache Commons HTTPClient.
 - Removed MantaCryptoException.
 - Removed signature cache TTL and HTTP transport configuration parameters because
   they are no longer relevant in the 3.0 implementation.  
### Fixed
 - [Convert IllegalArgumentException uses that are catching null to NPEs](https://github.com/joyent/java-manta/issues/126)
 - [Add capability to calculate a Content-MD5 on putting of objects](https://github.com/joyent/java-manta/issues/95) 

## [2.7.1] - 2016-11-11
### Added
 - Added id getter to MantaJobBuilder.Run. 
### Fixed
 - [Setting content type or metadata headers for multipart upload doesn't work](https://github.com/joyent/java-manta/issues/129)

## [2.7.0] - 2016-11-10
### Changed
 - Added `set -o pipefail` to multipart upload jobs so that we catch job failure
   states explicitly.
 - We now throw exceptions when ABORTED and UNKNOWN upload states are encountered
   when executing MantaMultipartManager.waitForCompletion().
 - We now explicitly handle InterruptedExceptions on MantaClient.close() by
   not exiting early and continuing to close all of the dangling resources.
 - Added check for SHA256 fingerprints. We error and display a message to inform
   the user to use a MD5 fingerprint for the time being.
 - We now store the multipart upload job id on the metadata of the metadata 
   object which allows us to get the job id without doing a listing.

## [2.6.0] - 2016-11-03
### Added
 - Added support for multipart uploads implemented as Manta jobs.
### Changed
 - Upgraded HTTP Signatures dependency.
 - Resolved numerous compiler warnings.
 - Improvements in recursive delete operations.
### Fixed
 - [Listing a directory that doesn't exist will result in an UncheckedIOException that is difficult to trace](https://github.com/joyent/java-manta/issues/119)
 - [Listing jobs will cause HTTP pool resource leaks](https://github.com/joyent/java-manta/issues/127)

## [2.5.0] - 2016-10-10
### Added
 - Setting HTTP Headers is now supported when making GET requests.
### Changed
 - Upgraded HTTP Signatures dependency.
 - Upgraded Apache HTTP Client.
 - Upgraded Google HTTP Client.
 - Resolved compiler warnings for MantaSSLSocketFactory and HttpRequestFactoryProvider.

## [2.4.3] - 2016-09-22
### Changed
 - Wait / notify behavior in MantaObjectOutputStream was improved for close()
   events.
### Fixed
 - [Fix the contributing link to work online on github. Case-Sensitive links](https://github.com/joyent/java-manta/issues/117)
 - [Minor pom fixes and plugin/dependency version updates](https://github.com/joyent/java-manta/issues/118) 

## [2.4.2]
Version skipped due to a release error.

## [2.4.1] - 2016-06-14
### Fixed
 - [Specifying key content will conflict with the default key path](https://github.com/joyent/java-manta/issues/116)

## [2.4.0] - 2016-06-01
### Added
 - MantaSeekableByteChannel now extends from InputStream.
 - Added move method to MantaClient.
 - MantaClient.putDirectory now returns a boolean value indicating the 
   successful creation of a new directory.
 - Added OutputStream implementation that allows for uploads via an
   OutputStream.
 - Added support for uploading using File objects, thereby getting retryable
   requests.
 - Added support for uploading byte arrays directly, thereby getting retryable
   requests.
 - Added logging of load balancer IP upon exception. This is implemented using
   the SLF4J MDC class.
### Changed
 - Putting strings now acts as a retryable request.
 - Fixed retrying so that it works consistently with Google HTTP Client.
 - We try to discover the mime-type from the filename when it isn't set as
   part of the header.
### Fixed
 - [Unable to parse ISO 8601 mtime value from ObjectResponse object](https://github.com/joyent/java-manta/issues/114)
 
## [2.3.0] - 2016-04-22
### Changed
 - Upgraded http-signatures to 2.2.0 so that we are keeping pace with upstream
   changes.
 - Rejiggered Maven shade settings to better exclude certain dependencies and
   shade others so that they won't cause conflicts.
 - Upgraded Apache Commons Collections to 4.1 because it has security patches.
 - Added Apache Commons Lang as a dependency.
 - Added additional details about HTTP requests to all exceptions.
 - Changed license to MPLv2.

## [2.2.3] - 2016-03-12
### Added
 - We catch IOExceptions and rethrow them with additional debug output appended.
 
### Changed
 - Upgraded http-signatures to 2.0.2 so that we support native gmp on SmartOS. 

## [2.2.2] - 2016-01-13
### Changed
 - Changed signature algorithm log statement to debug from info.
 - Change DNS resolver implementation to randomly rotate among the IPs
   returned from DNS for the Manta host when using the ApacheHttpTransport.

## [2.2.1] - 2016-01-07
### Added
 - Added logging message for signing algorithm.
 - Added x-request-id output on HTTP errors.
 - Add toString methods for ConfigContext.

## [2.2.0] - 2016-01-02
### Added
 - Added http transport library configuration parameter.
 - We now honor TLS cipher suite parameters (https.cipherSuites) when using
   ApacheHttpTransport.
 - We now honor TLS protocols parameters (https.protocols) when using
   ApacheHttpTransport.
 - We now allow configuration of TLS protocols and ciphers.
 - Upgraded http-signatures library to 1.1.0.
 - We now allow you to disable HTTP signature based authentication
   altogether.
 - We now allow you to disable native code cryptographic performance
   enhancements.
 - We now enable HTTP signature caching by default with a TTL of 1000ms, but
   it can be disabled by setting it to 0ms.

## [2.1.0] - 2015-12-28
### Added
 - Added maximum connections configuration parameter.

## [2.0.0] - 2015-12-21
### Changed
 - MantaException now inherits from RuntimeException.
 - MantaClientHttpResponseException now inherits from IOException.
 - MantaUtils.inputStreamToString now supports specifying a Charset.
 - Changed newInstance() methods to constructors in MantaClient.
 - Changed to a multi-module Maven project.
 - Broke out integration tests into a separate Maven module so that we can
   test classes in their shaded state.
 - Bumped minimum Java version to 1.8.
 - Changed to a JDK 8 Stream model for streamable resources.
 - Many utility methods used for making HTTP requests have been moved to the
   HttpHelper class.

### Added
 - Added read-only support for NIO SeekableByteChannel streams.
 - Added support for recursive directory creation.
 - Added support for home directory path look up in ConfigContext.
 - Added MantaMetadata class for handling object metadata.
 - Added MantaHttpHeaders class for handling custom HTTP headers.
 - Added build to Travis CI.
 - Added closeQuietly() to MantaClient.
 - Added parsing of JSON error responses from Manta API.
 - Added error response enum MantaErrorCode.
 - Added support for signed URLs.
 - Added support for RBAC roles.
 - Added Manta compute job support.
 - Added HTTP retries as a configuration parameter.
 - Added additional Maven enforcer checks.

### Removed
 - Removed all non-context based constructors from MantaClient.

## [1.6.0] - 2015-11-11
### Changed
 - Massive reformatting and clean up of the entire code base.
 - We now use Apache HTTP Client to make all of our HTTP requests.

### Added
 - Added additional documentation.
 - Added system properties configuration support.
 - Added chained configuration context support.
 - Added configuration context unit tests.

### Fixed
 - [HTTP Content-Type header is not being read correctly](https://github.com/joyent/java-manta/issues/39)
 - [README specifies incorrect system property name for manta user](https://github.com/joyent/java-manta/issues/48)
 - [manta client recursive delete does not work](https://github.com/joyent/java-manta/issues/49)
 - [manta client listObjects method does not completely populate manta objects](https://github.com/joyent/java-manta/issues/51)
 - [Convert MantaObject.getMtime() to use an actual Java time type](https://github.com/joyent/java-manta/issues/33)

## [1.5.4] - 2015-10-31
### Changed
 - Project came under new leadership.
 - Unit test framework changed to TestNG.
 - MantaClient is no longer declared final.
 - Changed developer information in pom.xml.
 - More code formatting clean up and javadoc changes.

### Added
 - Added MantaUtils test cases.
 - Added additional key file path sanity checks.

## [1.5.3] - 2015-10-28
### Changed
 - Migrated logger to use slf4j.
 - Upgraded google-http-client to 1.19.0
 - Moved Junit Maven scope to test.
 - Upgraded JDK to 1.7 and replaced Commons IO method calls with standard library method calls.
 - Upgraded bouncycastle libraries to 1.51.
 - Maven now creates a shaded version that includes everything except bouncycastle and slf4j.
 - Bumped Maven dependency versions.
 - Enabled Manta unit tests to draw their configuration from environment variables.
 - Updated release documentation.

### Added
 - Added nexus release plugin.

### Fixed
 - Fixed checkstyle failures.
 - Fixed pom.xml settings so that it will properly release to Maven Central.
 
 

 
