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

package edu.harvard.drs.remediation.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Amazon S3 remediation task tests.
 */
public class AmazonS3RemediationTaskTest extends AbstractTaskTest {

    @Test
    public void testExecute() {
        this.remediationTasks.get(0)
            .execute();

        for (int k = 0; k < keys[0].length; ++k) {
            verify(this.store, times(1))
                .rename(partitions.get(0).get(k), destinationKeys[0][k]);
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
            .remediate(partitions.get(0).get(1));

        verify(this.store, times(1))
            .rename(partitions.get(0).get(1), destinationKeys[0][1]);
    }

    @Test
    public void testRemediateSkipped() {
        S3Object object = S3Object.builder()
            .key(keys[0][0])
            .lastModified(now)
            .build();

        int actual = this.remediationTasks.get(0)
            .remediate(object);

        verify(this.store, times(1))
            .rename(object, destinationKeys[0][0]);

        assertEquals(1, actual);
    }

    @Test
    public void testRemediateSkippedModifiedAfter() {
        S3Object object = S3Object.builder()
            .key(keys[0][0])
            .lastModified(Instant.now().plus(10, ChronoUnit.SECONDS))
            .build();

        int actual = this.remediationTasks.get(0)
            .remediate(object);

        assertEquals(5, actual);
    }

    @Test
    public void testMapKey() {
        String mappedKey = this.remediationTasks.get(0)
            .mapKey(keys[0][0]);

        assertNotNull(mappedKey);
        assertEquals(destinationKeys[0][0], mappedKey);

        assertTrue(this.remediationTasks.get(0)
            .verifyRename(mappedKey));
    }

    @Test
    public void testVerifyRename() {
        String mappedKey = "";

        assertFalse(this.remediationTasks.get(0)
            .verifyRename(mappedKey));

        mappedKey = "12887296/v1/content/data/400171120.png";

        assertFalse(this.remediationTasks.get(0)
            .verifyRename(mappedKey));

        mappedKey = "6927/8821/12887296/v1/content/data/400171120.png";

        assertTrue(this.remediationTasks.get(0)
            .verifyRename(mappedKey));
    }

}
