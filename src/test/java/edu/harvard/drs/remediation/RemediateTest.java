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
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.harvard.drs.remediation.store.AbstractStoreTest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * DRS Remediate tests.
 */
public class RemediateTest extends AbstractStoreTest {

    @Test
    public void testMain(final S3Client s3) throws InterruptedException {
        Remediate.main(new String[] { endpointOverride });

        // unfortunately no better way to wait until main is complete
        Thread.sleep(45000);

        List<String> expectedRenamedKeys = Arrays.asList(new String[] {
            "1037/8821/12887301/v1/content/data/400171130.lfs",
            "1037/8821/12887301/v1/content/data/400171130.png",
            "1037/8821/12887301/v1/content/descriptor/400171130_mets.xml",
            "1037/8821/12887301/v1/content/metadata/400171130_mods.xml",
            "2037/8821/12887302/v1/content/data/400171132.lfs",
            "2037/8821/12887302/v1/content/data/400171132.png",
            "2037/8821/12887302/v1/content/descriptor/400171132_mets.xml",
            "2037/8821/12887302/v1/content/metadata/400171132_mods.xml",
            "5037/8821/12887305/v1/content/data/400171138.lfs",
            "5037/8821/12887305/v1/content/data/400171138.png",
            "5037/8821/12887305/v1/content/descriptor/400171138_mets.xml",
            "5037/8821/12887305/v1/content/metadata/400171138_mods.xml",
            "6927/8821/12887296/v1/content/data/400171120.lfs",
            "6927/8821/12887296/v1/content/data/400171120.png",
            "6927/8821/12887296/v1/content/descriptor/400171120_mets.xml",
            "6927/8821/12887296/v1/content/metadata/400171120_mods.xml",
            "9927/8821/12887299/v1/content/data/400171126.lfs",
            "9927/8821/12887299/v1/content/data/400171126.png",
            "9927/8821/12887299/v1/content/descriptor/400171126_mets.xml",
            "9927/8821/12887299/v1/content/metadata/400171126_mods.xml"
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
