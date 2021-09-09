# s3utils

## Environment

| Variable                | Description                                | Default                    |
| ----------------------- | ------------------------------------------ | -------------------------- |
| LOG_LEVEL               | root log level                             | info                       |
| REMEDIATION_LOG         | file path for remediation log              | ./external/remediation.log |
| INPUT_PATH              | file path for input data                   | ./external/dump.txt        |
| INPUT_PATTERN           | regex pattern per line to key-value pair   | ^\\d+ : (\\d+) .*:(\\d+)$  |
| INPUT_SKIP              | number of lines of input file to skip      | 2                          |
| PARALLELISM             | number of concurrent tasks                 | 12                         |
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

This task will first load specified input file and process each line with input pattern skipping number of lines specified by input skip. The two capture groups of the input pattern will populate an in memory lookup table.

Next an Amazon S3 bucket will be partitioned by the number of specified AWS list max keys. Each partition will be a list of S3 objects that may need to be remediated by renaming its key.

The remediation process:

1. parse root path from object key
2. remove any leading zeroes of root path
3. use root path as key for lookup for URN NNS value
4. replace root path with URN NSS value
5. copy object from source key to renamed destination key
6. delete source object

> If the key is not found in lookup table the remediation will be skipped for that object.

Each partition will be provided to a process task along with a lookup table and object store to be queued in a process queue until all objects have been processed.

Each object remediated will result in a row in the remediation log. Each row will consist of source object key, destination object key, object eTag, object size in bytes, result of rename (0 = success, 1 = skipped, -1 = s3 client/server error, -2 = copy incorrect etag), and ellapsed time in milliseconds. The remediation log will be appended on subsequent executions.

## Run

Build
```
mvn clean package
```

Run
```
java -jar target\s3utils-jar-with-dependencies.jar
```

## Docker

Build
```
docker build --no-cache -t s3utils .
```

Run
```
docker run -v /c/Users/wwelling/Development/harvard/s3utils/external:/external --env-file=.env s3utils
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
