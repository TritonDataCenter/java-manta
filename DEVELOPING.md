# Configuration Parameter Contributions

This is a brief guide for developers interested in making open-source contributions to the configuration parameters for 
java-manta. [Java-Manta]((http://joyent.github.com/java-manta)) is a community-maintained Java SDK for interacting with 
Joyent's Manta object storage system.

### Recommended Prerequisite References
 - [Using The SDK](https://github.com/joyent/java-manta#usage)
 - [Contributing to Java-Manta](https://github.com/joyent/java-manta/blob/master/CONTRIBUTING.md)
 - [Configuration Documentation](https://github.com/joyent/java-manta/blob/master/USAGE.md#configuration)
 - [General Examples](https://github.com/joyent/java-manta#general-examples)
 
### Adding a Configuration Parameter to the SDK

Configuration parameters allow users to flexibly leverage SDK features to match their performance, authentication, 
and encryption requirements.

To submit a new configuration parameter to the Java-Manta SDK:
 - Determine the nature of the new parameter. Consider basic design decisions such as conditions that govern its use, 
  its datatype, etc.
   - For example, if the new configuration parameter is a ```Boolean```, find the line ```Boolean isClientEncryptionEnabled();```
     in [ConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/ConfigContext.java). 
     Use that section of code as a template to implement the new configuration variable.

 - Add a method in [SettableConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/SettableConfigContext.java) 
  and imitate the pre-existing implementations in the class. Include additions to detailed information in the JavaDocs.

 - Add keys for constants in [MapConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/MapConfigContext.java) and [EnvVarConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/EnvVarConfigContext.java). 
  Include an implementation for the required interface methods.

 - Add a default setting in [DefaultsConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/DefaultsConfigContext.java). This determines a default value for the parameter when none is provided 
  via command line or environment variable.

 - Add code to the methods `overwriteWithContext()` and `overwriteWithDefaultContext()` in [BaseChainedConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/SettableConfigContext.java) 
  to ensure the overwrite behavior works correctly.
  
 - Add introduced configuration parameter in [AuthAwareConfigContext](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/AuthAwareConfigContext.java) and [ConfigContextMBean](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/main/java/com/joyent/manta/config/ConfigContextMBean.java).
   The latter determines which configuration parameters of the SDK are exposed leveraging the `MetricReporterSupplier` class using `JMX` or `SLF4J`. 
  
 - Update the test file [ChainedConfigContextTest](https://github.com/joyent/java-manta/blob/master/java-manta-client-unshaded/src/test/java/com/joyent/manta/config/ChainedConfigContextTest.java) to verify the behavior for the added parameter.
  
 - Update [USAGE.md](https://github.com/joyent/java-manta/blob/master/USAGE.md#configuration) to include the new configuration parameter and to add clear instructions on how to use it.

 - Add information about the new parameter in the [CHANGELOG.md](https://github.com/joyent/java-manta/blob/master/CHANGELOG.md).


 Note: [Adding a new configuration parameter manta.content_type_detection](https://github.com/joyent/java-manta/pull/494) can help developers find and edit the appropriate classes.