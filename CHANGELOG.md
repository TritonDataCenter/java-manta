# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

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
