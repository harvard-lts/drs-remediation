/**
 * Copyright (c) 2021 President and Fellows of Harvard College
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.harvard.drs.remediation;

import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsBucketName;
import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsMaxKeys;
import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsMaxPartSize;
import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsMultipartThreshold;
import static edu.harvard.drs.remediation.utility.EnvUtils.getAwsSkipMultipart;
import static edu.harvard.drs.remediation.utility.EnvUtils.getParallelism;
import static edu.harvard.drs.remediation.utility.RuntimeUtils.availableProcessors;
import static edu.harvard.drs.remediation.utility.RuntimeUtils.totalMemory;
import static edu.harvard.drs.remediation.utility.TimeUtils.elapsed;
import static java.lang.System.nanoTime;

import edu.harvard.drs.remediation.store.AmazonS3Bucket;
import edu.harvard.drs.remediation.store.ObjectStore;
import edu.harvard.drs.remediation.task.AmazonS3RemediationTask;
import edu.harvard.drs.remediation.task.Callback;
import edu.harvard.drs.remediation.task.IteratingTaskProcessor;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * DRS Remediate.
 */
@Slf4j
public final class Remediate {

    private Remediate() { }

    /**
     * S3 utils main entry point.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        log.debug("{} available processors", availableProcessors());
        log.debug("{} GiB total memory used by JVM", totalMemory());

        log.info("{} AWS bucket", getAwsBucketName());
        log.info("{} AWS max keys", getAwsMaxKeys());
        log.info("{} AWS max part size", getAwsMaxPartSize());
        log.info("{} AWS multipart threshold", getAwsMultipartThreshold());
        log.info("{} AWS skip multipart", getAwsSkipMultipart());

        log.info("{} parallelism", getParallelism());

        final String endpointOverride = args.length > 0 ? args[0] : null;

        final AmazonS3Bucket s3 = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            getAwsSkipMultipart(),
            endpointOverride
        );

        final long startTime = nanoTime();

        log.info("remediation of S3 bucket {} started", getAwsBucketName());

        final Instant start = Instant.now();

        Iterator<List<S3Object>> iterator = s3.iterator();

        new IteratingTaskProcessor<AmazonS3RemediationTask>(getParallelism(), new Iterator<AmazonS3RemediationTask>() {

            @Override
            public synchronized boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public synchronized AmazonS3RemediationTask next() {
                ObjectStore store = new AmazonS3Bucket(
                    getAwsBucketName(),
                    getAwsMaxKeys(),
                    getAwsMaxPartSize(),
                    getAwsMultipartThreshold(),
                    getAwsSkipMultipart(),
                    endpointOverride
                );

                List<S3Object> objects = iterator.next();

                return new AmazonS3RemediationTask(start, store, objects);
            }

        }, new Callback() {

            @Override
            public void complete() {
                log.info("remediation of S3 bucket {} completed in {} milliseconds",
                    getAwsBucketName(), elapsed(startTime));
                s3.close();
            }

        }).start();
    }

}
