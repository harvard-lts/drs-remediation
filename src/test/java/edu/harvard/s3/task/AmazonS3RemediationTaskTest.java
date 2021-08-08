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

package edu.harvard.s3.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Amazon S3 remediation task tests.
 */
public class AmazonS3RemediationTaskTest extends AbstractTaskTest {

    private final String key = "822376800000/v1/content/descriptor/822376800000_mets.xml";

    @Test
    public void testExecute() {
        this.remediationTasks.get(0)
            .execute();

        verify(this.lookup, times(3))
            .get(ids[0]);

        for (int k = 0; k < keys[0].length; ++k) {
            verify(this.store, times(1))
                .rename(partitions.get(0).get(k), destinationKey(0, k));
        }
    }

    @Test
    public void testComplete() {
        this.remediationTasks.get(0)
            .complete();

        verify(this.store, times(1))
            .close();
    }

    @Test
    public void testRemediate() {
        this.remediationTasks.get(0)
            .remediate(partitions.get(0).get(0));

        verify(this.lookup, times(1))
            .get(ids[0]);

        verify(this.store, times(1))
            .rename(partitions.get(0).get(0), destinationKey(0, 0));
    }

    @Test
    public void testRemediateSkipped() {
        S3Object object = S3Object.builder()
            .key(key)
            .build();

        this.remediationTasks.get(0)
            .remediate(object);

        verify(this.lookup, times(1))
            .get(key.split("/")[0]);

        verify(this.store, times(0))
            .rename(object, destinationKey(key));
    }

    @Test
    public void testMapKey() {
        String urn = this.remediationTasks.get(0)
            .mapKey(keys[0][0]);

        assertNotNull(urn);

        assertEquals("12887296/v1/content/data/400171120.png", urn);

        verify(this.lookup, times(1))
            .get(ids[0]);
    }

    @Test
    public void testMapKeyNotFound() {
        String urn = this.remediationTasks.get(0)
            .mapKey(key);

        assertNull(urn);
    }

}
