# Change Log
All notable changes to this project will be documented in this file.
This project aims to adhere to [Semantic Versioning](http://semver.org/).

## [2.5.0] - 2016-10-10
### Added
 - Setting HTTP Headers is now supported when making GET requests.
### Changed
 - Upgraded Google HTTP Client.

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
