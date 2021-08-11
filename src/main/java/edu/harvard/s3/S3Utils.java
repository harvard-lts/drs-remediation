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

package edu.harvard.s3;

import static edu.harvard.s3.utility.EnvUtils.getAwsBucketName;
import static edu.harvard.s3.utility.EnvUtils.getAwsMaxKeys;
import static edu.harvard.s3.utility.EnvUtils.getAwsMaxPartSize;
import static edu.harvard.s3.utility.EnvUtils.getAwsMultipartThreshold;
import static edu.harvard.s3.utility.EnvUtils.getAwsSkipMultipart;
import static edu.harvard.s3.utility.EnvUtils.getInputPath;
import static edu.harvard.s3.utility.EnvUtils.getInputPattern;
import static edu.harvard.s3.utility.EnvUtils.getInputSkip;
import static edu.harvard.s3.utility.EnvUtils.getParallelism;
import static edu.harvard.s3.utility.RuntimeUtils.availableProcessors;
import static edu.harvard.s3.utility.RuntimeUtils.totalMemory;
import static edu.harvard.s3.utility.TimeUtils.elapsed;
import static java.lang.System.nanoTime;

import edu.harvard.s3.loader.FileLoader;
import edu.harvard.s3.lookup.InMemoryLookupTable;
import edu.harvard.s3.store.AmazonS3Bucket;
import edu.harvard.s3.store.ObjectStore;
import edu.harvard.s3.task.AmazonS3RemediationTask;
import edu.harvard.s3.task.Callback;
import edu.harvard.s3.task.ProcessTaskQueue;
import lombok.extern.slf4j.Slf4j;

/**
 * S3 utilities.
 */
@Slf4j
public final class S3Utils {

    private S3Utils() { }

    /**
     * S3 utils main entry point.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) {
        log.debug("{} available processors", availableProcessors());
        log.debug("{} GiB total memory used by JVM", totalMemory());

        final String inputPath = args.length > 0 ? args[0] : getInputPath();

        final String endpointOverride = args.length > 1 ? args[1] : null;

        final FileLoader loader = new FileLoader(inputPath, getInputPattern(), getInputSkip());
        final InMemoryLookupTable lookup = new InMemoryLookupTable(loader);

        final AmazonS3Bucket s3 = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            getAwsSkipMultipart(),
            endpointOverride
        );

        final long startTime = nanoTime();

        final ProcessTaskQueue processTaskQueue = new ProcessTaskQueue(getParallelism(), new Callback() {

            @Override
            public void complete() {
                log.info("remediation of S3 bucket {} completed in {} milliseconds",
                    getAwsBucketName(), elapsed(startTime));
                log.info("{} objects in bucket {} after remediation", s3.count(), getAwsBucketName());
                lookup.unload();
                s3.close();
            }

        });

        lookup.load();

        log.info("remediation of S3 bucket {} started", getAwsBucketName());

        s3.partition().forEach(objects -> {
            ObjectStore store = new AmazonS3Bucket(
                getAwsBucketName(),
                getAwsMaxKeys(),
                getAwsMaxPartSize(),
                getAwsMultipartThreshold(),
                getAwsSkipMultipart(),
                endpointOverride
            );
            processTaskQueue.submit(new AmazonS3RemediationTask(store, lookup, objects));
        });
    }

}
