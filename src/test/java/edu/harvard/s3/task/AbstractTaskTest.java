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

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import edu.harvard.s3.lookup.LookupTable;
import edu.harvard.s3.store.ObjectStore;
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

    @Mock
    LookupTable<String, String> lookup;

    final List<ObjectStore> objectStores = new ArrayList<>();

    final List<LookupTable<String, String>> lookupStores = new ArrayList<>();

    final List<AmazonS3RemediationTask> remediationTasks = new ArrayList<>();

    final String[] ids = new String[] {
        "400171120",
        "400171138"
    };

    final String[] nss = new String[] {
        "12887296",
        "12887305"
    };

    final String[][] keys = new String[][] {
        {
            "0400171120/v1/content/data/400171120.png",
            "0400171120/v1/content/descriptor/400171120_mets.xml",
            "0400171120/v1/content/metadata/400171120_mods.xml"
        },
        {
            "0400171138/v1/content/data/400171138.png",
            "0400171138/v1/content/descriptor/400171138_mets.xml",
            "0400171138/v1/content/metadata/400171138_mods.xml"
        }
    };

    final List<List<S3Object>> partitions = new ArrayList<>();

    @BeforeEach
    void setup() {
        this.objectStores.add(this.store);
        this.objectStores.add(this.store);
        this.lookupStores.add(this.lookup);
        this.lookupStores.add(this.lookup);

        for (int i = 0; i < ids.length; ++i) {
            List<S3Object> partition = new ArrayList<>();
            partition.add(object(keys[i][0]));
            partition.add(object(keys[i][1]));
            partition.add(object(keys[i][2]));
            partitions.add(partition);

            ObjectStore store = this.objectStores.get(i);

            LookupTable<String, String> lookup = this.lookupStores.get(i);

            doReturn(nss[i])
                .when(lookup)
                .get(ids[i]);

            doNothing()
                .when(store)
                .close();

            for (int k = 0; k < keys[0].length; ++k) {
                doReturn(0)
                    .when(store)
                    .rename(partition.get(k), destinationKey(i, k));
            }

            this.remediationTasks.add(new AmazonS3RemediationTask(store, lookup, partition));
        }
    }

    String destinationKey(int i, int k) {
        return keys[i][k].replace("0" + ids[i], nss[i]);
    }

    String destinationKey(String key) {
        return key.replace("0" + ids[0], nss[0]);
    }

    private S3Object object(String key) {
        return S3Object.builder()
            .key(key)
            .build();
    }

}
