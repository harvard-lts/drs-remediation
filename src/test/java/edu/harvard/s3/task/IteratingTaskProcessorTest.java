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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import edu.harvard.s3.lookup.LookupTable;
import edu.harvard.s3.store.ObjectStore;
import org.junit.jupiter.api.Test;

/**
 * Iterating task processor tests.
 */
public class IteratingTaskProcessorTest extends AbstractTaskTest {

    @Test
    public void testProcessor() {
        Callback callback = mock(Callback.class);

        new IteratingTaskProcessor<AmazonS3RemediationTask>(1, this.remediationTasks.iterator(), callback).start();

        verify(callback, timeout(1000).times(1))
            .complete();

        for (int i = 0; i < ids.length; ++i) {
            ObjectStore store = this.objectStores.get(i);

            LookupTable<String, String> lookup = this.lookupStores.get(i);

            verify(lookup, timeout(100).times(3))
                .get(ids[i]);

            for (int k = 0; k < keys[0].length; ++k) {
                verify(store, timeout(100).times(1))
                    .rename(partitions.get(i).get(k), destinationKey(i, k));
            }
        }
    }

}
