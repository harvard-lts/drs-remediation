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

package edu.harvard.s3.store;

import static edu.harvard.s3.utility.EnvUtils.getAwsBucketName;
import static edu.harvard.s3.utility.EnvUtils.getAwsMaxKeys;
import static edu.harvard.s3.utility.EnvUtils.getAwsMaxPartSize;
import static edu.harvard.s3.utility.EnvUtils.getAwsMultipartThreshold;
import static edu.harvard.s3.utility.EnvUtils.getAwsSkipMultipart;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * Amazon S3 bucket tests.
 */
public class AmazonS3BucketTest extends AbstractStoreTest {

    @Test
    public void testMalformedUri() {
        assertThrows(RuntimeException.class, () -> {
            new AmazonS3Bucket(
                getAwsBucketName(),
                getAwsMaxKeys(),
                getAwsMaxPartSize(),
                getAwsMultipartThreshold(),
                getAwsSkipMultipart(),
                "xp:\\fubar/|*/%~?foo-bar=test?/2021"
            );
        });
    }

    @Test
    public void testCount() {
        AmazonS3Bucket store = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            getAwsSkipMultipart(),
            endpointOverride
        );

        int count = store.count();

        assertEquals(20, count);

        store.close();
    }

    @Test
    public void testPartition() {
        AmazonS3Bucket store = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            getAwsSkipMultipart(),
            endpointOverride
        );

        List<List<S3Object>> paritions = store.partition()
            .collect(Collectors.toList());

        assertEquals(1, paritions.size());
        assertEquals(20, paritions.get(0).size());

        store.close();
    }

    @Test
    public void testRename(final S3Client s3) {
        AmazonS3Bucket store = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            getAwsSkipMultipart(),
            endpointOverride
        );

        List<List<S3Object>> paritions = store.partition()
            .collect(Collectors.toList());

        List<S3Object> originalObjects = paritions.get(0);

        List<String> expectedRenamedKeys = new ArrayList<>();

        for (S3Object object : originalObjects) {
            String[] parts = object.key().split(Pattern.quote("."));
            String destinationKey = parts[0] + "_renamed." + parts[1];
            int result = store.rename(object, destinationKey);
            assertEquals(0, result);
            expectedRenamedKeys.add(destinationKey);
        }

        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(getAwsBucketName())
            .build();

        ListObjectsV2Iterable iterable = s3.listObjectsV2Paginator(request);

        List<S3Object> renamedObjects = iterable.contents()
            .stream()
            .collect(Collectors.toList());

        assertEquals(originalObjects.size(), renamedObjects.size());

        for (int i = 0; i < renamedObjects.size(); ++i) {
            S3Object originalObject = originalObjects.get(i);
            S3Object renamedObject = renamedObjects.get(i);
            String expectedRenamedKey = expectedRenamedKeys.get(i);

            assertEquals(expectedRenamedKey, renamedObject.key());
            assertEquals(originalObject.eTag(), renamedObject.eTag());
        }

        store.close();
    }

    @Test
    public void testRenameSkipLargeObject(final S3Client s3) {
        AmazonS3Bucket store = new AmazonS3Bucket(
            getAwsBucketName(),
            getAwsMaxKeys(),
            getAwsMaxPartSize(),
            getAwsMultipartThreshold(),
            true,
            endpointOverride
        );

        S3Object object = S3Object.builder()
            .size(5368709121L)
            .key("foo")
            .build();

        int result = store.rename(object, "bar");

        assertEquals(1, result);
    }

}
