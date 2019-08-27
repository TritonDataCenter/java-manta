## Description

Documentation substantiating the java-manta CLI support for Manta buckets. It will potentially contain information needed
for developers to interact with Manta Buckets leveraging the Java SDK

### Operations

Citing [RFD#155](https://github.com/joyent/rfd/blob/master/rfd/0155/README.md#rfd-155-manta-buckets-api), the operations for buckets are as follows:
- create bucket.
- list buckets.
- delete bucket.
- head bucket.
- list objects.
- head object.
- put object(s).
- get object(s).
- delete object(s).

Note: Some complex operations will be potentially included in later stages:
- destroy bucket
- move bucket
- check whether bucket is empty.

### AWS S3 Java SDK:

```
- s3.createBucket(bucket_name).                                      1. create bucket
- s3.listBuckets().                                                  2. list buckets
- s3.deleteBucket(bucket_name).                                      3. delete bucket
- s3.doesBucketExist(bucket_name).                                   4. head bucket
- s3.listObjects(bucket_name).                                       5. list objects
- s3.putObject(bucket_name, key_name, new File(file_path)).          6. put object(s)
- s3.getObject(bucket_name, key_name).                               7. get object(s)
- s3.deleteObject(bucket_name, summary.getKey()).                    8. delete object
- s3.deleteObjects(deleteObjectRequest).                             9. delete object(s)
- s3.copyObject(from_bucket, object_key, to_bucket, object_key).     10. copy object
```

### Manta Java SDK.

```
- Create bucket:           manta.createBucket(bucket_path).
- List buckets:            manta.listBuckets().
- Delete bucket:           manta.deleteBucket(bucket_path).
- head bucket:             manta.exitsAndIsAccessible(bucket_path).
- Options for buckets:     manta.options(buckets_root_dir).
- list objects:            manta.listBucketObjects(bucket_path).
- head object:             manta.exitsAndIsAccessible(object_path).
- put object(s).           manta.put(bucket_path, new File(file_path))
- get object(s):           manta.get(object_path)
- delete object(s):        manta.delete(object_path)
```


