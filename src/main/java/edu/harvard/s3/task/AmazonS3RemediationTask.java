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

import static java.util.Objects.nonNull;

import edu.harvard.s3.lookup.LookupTable;
import edu.harvard.s3.store.ObjectStore;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Amazon S3 remediation task to rename object source key using provided lookup
 * table.
 */
@Slf4j
public class AmazonS3RemediationTask implements ProcessTask {

    private static final Logger remediation = LoggerFactory.getLogger("remediation");

    private static final String ZERO = "0";

    private static final String PATH_SEPARATOR = "/";

    private final ObjectStore s3;

    private final LookupTable<String, String> lookup;

    private final List<S3Object> objects;

    /**
     * Amazon S3 remediation task constructor.
     *
     * @param s3      object store to remediate
     * @param lookup  lookup table used to rename object keys
     * @param objects list of S3 objects to remediate
     */
    public AmazonS3RemediationTask(
        ObjectStore s3,
        LookupTable<String, String> lookup,
        List<S3Object> objects
    ) {
        this.s3 = s3;
        this.lookup = lookup;
        this.objects = objects;
    }

    @Override
    public ProcessTask execute() {
        this.objects.forEach(this::remediate);

        return this;
    }

    @Override
    public void complete() {
        this.s3.close();
    }

    /**
     * Remediate S3 object key by renaming with DRS id replaced by URN NSS.
     *
     * @param object S3 object
     */
    void remediate(S3Object object) {
        String destinationKey = mapKey(object.key());

        if (nonNull(destinationKey)) {
            int result = this.s3.rename(object, destinationKey);
            remediation.info("{},{},{},{},{}",
                object.key(), destinationKey, object.eTag(), object.size(), result);
        }
    }

    /**
     * Replace root "folder" DRS id with URN NSS.
     *
     * @param key object key
     * @return remediated object key
     */
    String mapKey(String key) {
        // parse root "folder" from object key
        String iid = key.substring(0, key.indexOf(PATH_SEPARATOR));
        String id = iid;

        // remove leading zeroes
        while (id.startsWith(ZERO)) {
            id = id.substring(1);
        }

        // lookup root "folder" as id
        String nss = this.lookup.get(id);

        if (nonNull(nss)) {
            // replace root "folder", DRS id, with URN NSS of object key
            return key.replace(iid + PATH_SEPARATOR, nss + PATH_SEPARATOR);
        } else {
            log.warn("key {} not found in lookup table, skipping {}", id, key);
        }

        return null;
    }

}