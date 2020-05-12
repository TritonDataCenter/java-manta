## Recommended Prerequisite References
- [MantaCLI Requirements for usage](https://github.com/joyent/java-manta/blob/master/USAGE.md#cli-requirements). 
- [Enabling libnss Support via PKCS11 for Java 8 & Java 11](https://github.com/joyent/java-manta/blob/master/USAGE.md#enabling-libnss-support-via-pkcs11).
- [Improving SDK encryption performance for uploads](https://github.com/joyent/java-manta/blob/master/USAGE.md#improving-encryption-performance).
- [Overriding default cryptographic provider ranking](https://github.com/joyent/java-manta/blob/master/USAGE.md#overriding-default-cryptographic-provider-ranking)

## MantaCLI Command Usage
The following list of commands that have been offered by java-manta's `MantaCLI` package that will enable the customer to 
leverage basic Manta operations. The operations introduced with [java-manta#566](https://github.com/joyent/java-manta/pull/566) 
along with the existing operations are enumerated below:

### Get-File:

- For regular downloads without encryption
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar get-file -o {path-to-local-file-in-disk} {path-to-file-in-Manta}
```

- For regular downloads using encryption
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar get-file -cse -o {path-to-local-file-in-disk} {path-to-file-in-Manta}
```

- For range-downloads downloads without encryption.
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar get-file --start-bytes=0 --end-bytes=465615 -o {path-to-local-file-in-disk} {path-to-file-in-Manta}
```

- For range-downloads downloads using encryption.
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar get-file -cse --start-bytes=0 --end-bytes=465615 -o {path-to-local-file-in-disk} {path-to-file-in-Manta}
```

*Note:* Flag `--cse`/ `--using-encryption` is used to enable client-side encryption for java-manta.

### Put-File

- For uploads without encryption
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar put-file {path-to-local-file-in-disk} {path-to-file-in-Manta}

Creating new connection object
  com.joyent.manta.client.MantaClient@6bedbc4d
Attempting PUT request to: /Users/ashwinnair/test/header-error-logs-1.txt
com.joyent.manta.client.MantaObjectResponse{path='ashwin.nair/stor/test', contentLength=null, Request was successful
```

- For uploads using encryption
```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar put-file -cse {path-to-local-file-in-disk} {path-to-file-in-Manta}

Creating new connection object
  com.joyent.manta.client.MantaClient@6bedbc4d
Attempting PUT request to: /Users/ashwinnair/test/header-error-logs-1.txt
com.joyent.manta.client.MantaObjectResponse{path='ashwin.nair/stor/test', contentLength=null, Request was successful
```

*Note:* Flag `--cse`/ `--using-encryption` is used to enable client-side encryption for java-manta.

### Encryption-Config

- Default encryption configuration that can be overridden by system/environment configuration. Encryption configuration
  of the SDK utilized to perform uploads and downloads using MantaCLI.

```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar cse-config                  
BaseChainedConfigContext{mantaURL='https://us-east.manta.joyent.com', user='ashwin.nair', mantaKeyId=$mantaKeyId, mantaKeyPath='/Users/ashwinnair/.ssh/id_rsa', 
timeout=4000, retries=3, maxConnections=24, httpBufferSize='4096', httpsProtocols='TLSv1.2', httpsCiphers='TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA256', 
tlsInsecure=false, noAuth=false, disableNativeSignatures=false, tcpSocketTimeout=20000, connectionRequestTimeout=1000, verifyUploads=true, uploadBufferSize=16384, skipDirectoryDepth=null, pruneEmptyParentDepth=null, 
downloadContinuations=0, metricReporterMode=null, metricReporterOutputInterval=null, clientEncryptionEnabled=true, contentTypeDetectionEnabled=false, permitUnencryptedDownloads=true, encryptionAuthenticationMode=Optional, 
encryptionKeyId=manta-cli-encryption-key, encryptionAlgorithm=AES128/CTR/NoPadding, encryptionPrivateKeyPath=null, encryptionPrivateKeyBytesLength=32}
```

### Delete-File

- To delete a single object

```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar rm {path-to-file-in-Manta} 
Creating connection configuration
Creating new connection object
  com.joyent.manta.client.MantaClient@31e5415e
Attempting DELETE request to: ashwin.nair/stor/MANTA-5143
Request was successful
```

- To prune all empty parent directories:

```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar rm -d=-1 {path-to-file-in-Manta} 
Creating connection configuration
Creating new connection object
  com.joyent.manta.client.MantaClient@31e5415e
Attempting DELETE request to: ashwin.nair/stor/MANTA-5143
Request was successful
```

- To delete recursively all contents of a path/directory.

```
java -jar ./java-manta-cli/target/java-manta-cli-3.5.0-jar-with-dependencies.jar rm -r  {path-to-file-in-Manta} 
Creating connection configuration
Creating new connection object
  com.joyent.manta.client.MantaClient@31e5415e
Attempting DELETE request to: ashwin.nair/stor/MANTA-5143
Request was successful
```

### Listing Command:

- To list content(s) of a particular directory

```
java -jar java-manta-cli-3.5.0-jar-with-dependencies.jar ls ashwin.nair/stor/
  ashwin.nair/stor/.joyent
  ashwin.nair/stor/{object-uuid-1}
  ashwin.nair/stor/books
  ashwin.nair/stor/{object-uuid-2}
  ashwin.nair/stor/foo.txt
  ashwin.nair/stor/manta-monitor-data
  ashwin.nair/stor/name.txt
  ashwin.nair/stor/test
  ashwin.nair/stor/test-configuration.json
```

### Dump-Config

- Shows configuration used for configuring the Manta client.

```
java -jar java-manta-cli-3.5.0-jar-with-dependencies.jar dump-config                                                                         
BaseChainedConfigContext{mantaURL='https://us-east.manta.joyent.com', user='ashwin.nair', mantaKeyId='$mantaKeyId', mantaKeyPath='$mantaKeyPath', 
timeout=4000, retries=3, maxConnections=24, httpBufferSize='4096', httpsProtocols='TLSv1.2', httpsCiphers='TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA256', 
tlsInsecure=false, noAuth=false, disableNativeSignatures=false, tcpSocketTimeout=20000, connectionRequestTimeout=1000, verifyUploads=true, uploadBufferSize=16384, skipDirectoryDepth=null, pruneEmptyParentDepth=null, 
downloadContinuations=0, metricReporterMode=null, metricReporterOutputInterval=null, clientEncryptionEnabled=false, contentTypeDetectionEnabled=false, permitUnencryptedDownloads=false, encryptionAuthenticationMode=Mandatory, 
encryptionKeyId=null, encryptionAlgorithm=AES/CTR/NoPadding, encryptionPrivateKeyPath=null, encryptionPrivateKeyBytesLength=null object}
``` 

### Connect-Test

- Attempts connecting to Manta using system properties and environment variables for configuration.

```
java -jar java-manta-cli-3.5.0-jar-with-dependencies.jar connect-test
Creating connection configuration
  BaseChainedConfigContext{mantaURL='https://us-east.manta.joyent.com', user='ashwin.nair', mantaKeyId='SHA256:WNrlQjq45p/NOs04+mh8HcD6cXFeOSV8saf/AetOAoY', mantaKeyPath='/Users/ashwinnair/.ssh/id_rsa', 
  timeout=4000, retries=3, maxConnections=24, httpBufferSize='4096', httpsProtocols='TLSv1.2', httpsCiphers='TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_128_GCM_SHA256,TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_RSA_WITH_AES_128_CBC_SHA256', 
  tlsInsecure=false, noAuth=false, disableNativeSignatures=false, tcpSocketTimeout=20000, connectionRequestTimeout=1000, verifyUploads=true, uploadBufferSize=16384, skipDirectoryDepth=null, pruneEmptyParentDepth=null, 
  downloadContinuations=0, metricReporterMode=null, metricReporterOutputInterval=null, clientEncryptionEnabled=false, contentTypeDetectionEnabled=false, permitUnencryptedDownloads=false, encryptionAuthenticationMode=Mandatory, 
  encryptionKeyId=null, encryptionAlgorithm=AES/CTR/NoPadding, encryptionPrivateKeyPath=null, encryptionPrivateKeyBytesLength=null object}
Creating new connection object
  com.joyent.manta.client.MantaClient@5d7148e2
Attempting HEAD request to: /ashwin.nair
  com.joyent.manta.client.MantaObjectResponse{path='/ashwin.nair', contentLength=null, contentType='application/x-json-stream; type=directory', etag='null', mtime='null', type='directory', requestId='22d704e7-5488-4a3f-b683-bf5e86d4f5cc', httpHeaders=MantaHttpHeaders{wrappedHeaders={x-response-time=83, x-server-name=02d02889-cd80-4ac1-bc0c-4775b86661e4, x-request-id=22d704e7-5488-4a3f-b683-bf5e86d4f5cc, content-type=application/x-json-stream; type=directory, x-load-balancer=165.225.164.26, date=Tue, 12 May 2020 18:42:35 GMT, server=Manta/2, result-set-size=4}}, directory=true}
Request was successful
```

### Generate-Key

- Generates a client-side encryption key with the specified cipher and bits at the specified path.

```
java -jar java-manta-cli-3.5.0-jar-with-dependencies.jar generate-key AES 256 {path-to-write-encryption-key}
Generating key
Writing [AES-256] key to [$path-of-encryption-key]
```

### Validate-Key

- Validates that the supplied key is supported by the client-side encryption offered by java-manta.

```
java -jar java-manta-cli-3.5.0-jar-with-dependencies.jar validate-key AES {path-of-encryption-key}
Loading key from path [$path-of-encryption-key]
Cipher of key is [AES] as expected
Key format is [RAW]
```


