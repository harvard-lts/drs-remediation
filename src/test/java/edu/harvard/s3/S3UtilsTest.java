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
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.harvard.s3.store.AbstractStoreTest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * S3 utilies tests.
 */
public class S3UtilsTest extends AbstractStoreTest {

    @Test
    public void testMain(final S3Client s3) throws InterruptedException {
        S3Utils.main(new String[] { inputPath, endpointOverride });

        // unfortunately no better way to wait until main is complete
        Thread.sleep(75000);

        List<String> expectedRenamedKeys = Arrays.asList(new String[] {
            "12887296/v1/content/data/400171120.lfs",
            "12887296/v1/content/data/400171120.png",
            "12887296/v1/content/descriptor/400171120_mets.xml",
            "12887296/v1/content/metadata/400171120_mods.xml",
            "12887299/v1/content/data/400171126.lfs",
            "12887299/v1/content/data/400171126.png",
            "12887299/v1/content/descriptor/400171126_mets.xml",
            "12887299/v1/content/metadata/400171126_mods.xml",
            "12887301/v1/content/data/400171130.lfs",
            "12887301/v1/content/data/400171130.png",
            "12887301/v1/content/descriptor/400171130_mets.xml",
            "12887301/v1/content/metadata/400171130_mods.xml",
            "12887302/v1/content/data/400171132.lfs",
            "12887302/v1/content/data/400171132.png",
            "12887302/v1/content/descriptor/400171132_mets.xml",
            "12887302/v1/content/metadata/400171132_mods.xml",
            "12887305/v1/content/data/400171138.lfs",
            "12887305/v1/content/data/400171138.png",
            "12887305/v1/content/descriptor/400171138_mets.xml",
            "12887305/v1/content/metadata/400171138_mods.xml"
        });

        ListObjectsV2Request request = ListObjectsV2Request.builder()
            .bucket(getAwsBucketName())
            .build();

        ListObjectsV2Iterable iterable = s3.listObjectsV2Paginator(request);

        List<S3Object> renamedObjects = iterable.contents()
            .stream()
            .collect(Collectors.toList());

        assertEquals(expectedRenamedKeys.size(), renamedObjects.size());

        for (int i = 0; i < renamedObjects.size(); ++i) {
            S3Object renamedObject = renamedObjects.get(i);
            String expectedRenamedKey = expectedRenamedKeys.get(i);
            assertEquals(expectedRenamedKey, renamedObject.key());
        }
    }

}
