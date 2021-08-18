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

import java.util.List;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Object store interface.
 */
public interface ObjectStore {

    /**
     * Count objects in the bucket.
     *
     * @return bucket total count
     */
    int count();

    /**
     * Partition the object store.
     *
     * @return list of object partitions
     */
    List<List<S3Object>> partition();

    /**
     * Rename object source key with destination key.
     * Return status code:
     *   0 = success
     *   1 = skipped
     *  -1 = s3 client/server error
     *  -2 = copy incorrect etag
     *
     * @param source         source object to rename
     * @param destinationKey desired name of source object
     *
     * @return status code
     */
    int rename(S3Object source, String destinationKey);

    /**
     * Close object store.
     */
    void close();

}
