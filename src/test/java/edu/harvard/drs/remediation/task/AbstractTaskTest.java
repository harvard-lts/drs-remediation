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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import edu.harvard.drs.remediation.store.ObjectStore;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Abstract task tests setup.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public abstract class AbstractTaskTest {

    @Mock
    ObjectStore store;

    final Instant now = Instant.now();

    final List<ObjectStore> objectStores = new ArrayList<>();

    final List<AmazonS3RemediationTask> remediationTasks = new ArrayList<>();

    final String[] ids = new String[] {
        "12887296",
        "12887305"
    };

    final String[][] keys = new String[][] {
        {
            "12887296/v1/content/data/400171120.png",
            "12887296/v1/content/descriptor/400171120_mets.xml",
            "12887296/v1/content/metadata/400171120_mods.xml"
        },
        {
            "12887305/v1/content/data/400171138.png",
            "12887305/v1/content/descriptor/400171138_mets.xml",
            "12887305/v1/content/metadata/400171138_mods.xml"
        }
    };

    final String[][] destinationKeys = new String[][] {
        {
            "6927/8821/12887296/v1/content/data/400171120.png",
            "6927/8821/12887296/v1/content/descriptor/400171120_mets.xml",
            "6927/8821/12887296/v1/content/metadata/400171120_mods.xml"
        },
        {
            "5037/8821/12887305/v1/content/data/400171138.png",
            "5037/8821/12887305/v1/content/descriptor/400171138_mets.xml",
            "5037/8821/12887305/v1/content/metadata/400171138_mods.xml"
        }
    };

    final List<List<S3Object>> partitions = new ArrayList<>();

    @BeforeEach
    void setup() {
        this.objectStores.add(this.store);
        this.objectStores.add(this.store);

        for (int i = 0; i < ids.length; ++i) {
            List<S3Object> partition = new ArrayList<>();
            partition.add(object(keys[i][0]));
            partition.add(object(keys[i][1]));
            partition.add(object(keys[i][2]));
            partitions.add(partition);

            ObjectStore store = this.objectStores.get(i);

            doNothing()
                .when(store)
                .close();

            for (int k = 0; k < keys[0].length; ++k) {
                int result = keys[i][0].equals("12887296/v1/content/data/400171120.png")
                    ? 1
                    : 0;
                doReturn(result)
                    .when(store)
                    .rename(partition.get(k), destinationKeys[i][k]);
            }

            final Instant start = Instant.now();

            this.remediationTasks.add(new AmazonS3RemediationTask(start, store, partition));
        }
    }

    private S3Object object(String key) {
        return S3Object.builder()
            .key(key)
            .lastModified(now)
            .build();
    }

}
