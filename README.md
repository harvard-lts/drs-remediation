# drs-remediation

## Environment

| Variable                | Description                                | Default                    |
| ----------------------- | ------------------------------------------ | -------------------------- |
| LOG_LEVEL               | root log level                             | info                       |
| REMEDIATION_LOG         | file path for remediation log              | ./external/remediation.log |
| PARALLELISM             | number of concurrent tasks                 | 12                         |
| VERIFY_ONLY             | whether to only verify remediation         | false                      |
| AWS_BUCKET_NAME         | AWS S3 bucket name                         | harvard-drs-delivery       |
| AWS_MAX_KEYS            | AWS S3 list max keys                       | 1000                       |
| AWS_MAX_PART_SIZE       | AWS S3 max part size                       | 52428800 (50 MiB)          |
| AWS_MULTIPART_THRESHOLD | AWS S3 multipart threashold                | 104857600 (100 MiB)        |
| AWS_SKIP_MULTIPART      | AWS S3 skip if multipart threshold reached | false                      |
| AWS_REGION              | AWS region                                 | see ~/.aws/config          |
| AWS_ACCESS_KEY_ID       | AWS access key id                          | see ~/.aws/credentials     |
| AWS_SECRET_ACCESS_KEY   | AWS secret access key                      | see ~/.aws/credentials     |

## Tasks

### Amazon S3 Remediation Task

This task will partition an Amazon S3 bucket by the number of specified AWS list max keys. Each partition will be a list of S3 objects that may need to be remediated by renaming its key.

The remediation process:

1. parse root (URN NSS) path from object key
2. reverse URN NSS
3. prepend two additional "directory", <first-4-chars-of-reversed-nss>/<second-4-chars-of-reversed-nss>/<nss>/some/path
4. copy object from source key to renamed destination key
5. delete source object

Each partition will be provided to a process task along with a object store to be queued in a process queue until all objects have been processed.

Each object remediated will result in a row in the remediation log. ***The remediation log will be appended on subsequent executions.***

Remediation log:

```csv
source object key, destination object key, object eTag, object size in bytes, result of rename, and ellapsed time in milliseconds
```

The result flag:

```txt
-2 copy incorrect etag
-1 s3 client/server error
 0 success
 1 skipped due to multipart/threshold
 2 skipped due to unsupported key
```

## Run

Build
```
mvn clean package
```

Run
```
java -jar target\drs-remediate-jar-with-dependencies.jar
```

## Docker

Build
```
docker build --no-cache -t drs-remediate .
```

Run
```
docker run -v /c/drs-remediation/external:/external --env-file=.env drs-remediate
```

## Docker Compose

Build
```
docker-compose build --no-cache
```

Run
```
docker-compose up
```

> If using docker, do not forget to copy `.env.example` to `.env` and update accordingly.
