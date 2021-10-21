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

import static edu.harvard.drs.remediation.utility.EnvUtils.getVerifyOnly;
import static edu.harvard.drs.remediation.utility.TimeUtils.elapsed;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.isNumeric;
import static org.apache.commons.lang3.StringUtils.leftPad;
import static org.apache.commons.lang3.StringUtils.reverse;

import edu.harvard.drs.remediation.store.ObjectStore;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Amazon S3 remediation task to rename object source key using provided lookup
 * table.
 */
public class AmazonS3RemediationTask implements ProcessTask {

    private static final Logger remediation = LoggerFactory.getLogger("remediation");

    private static final String PATH_SEPARATOR = "/";

    private final ObjectStore s3;

    private final List<S3Object> objects;

    private final String id;

    private final boolean verifyOnly;

    /**
     * Amazon S3 remediation task constructor.
     *
     * @param s3      object store to remediate
     * @param objects list of S3 objects to remediate
     */
    public AmazonS3RemediationTask(
        ObjectStore s3,
        List<S3Object> objects
    ) {
        this.s3 = s3;
        this.objects = objects;
        this.id = UUID.randomUUID().toString();
        this.verifyOnly = getVerifyOnly();
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

    @Override
    public String id() {
        return id;
    }

    /**
     * Remediate S3 object key by renaming.
     *
     * @param object S3 object
     * @return result of rename
     */
    int remediate(S3Object object) {
        int result = 0;

        long startTime = System.nanoTime();

        String destinationKey = null;

        try {
            destinationKey = mapKey(object.key());
        } catch (NumberFormatException e) {
            result = 2;
        }

        if (destinationKey.equals(object.key())) {
            result = 3;
        } else if (nonNull(destinationKey)) {
            result = this.s3.rename(object, destinationKey);
        }

        remediation.info("{},{},{},{},{},{}",
            object.key(), destinationKey, object.eTag(), object.size(), result, elapsed(startTime));

        return result;
    }

    /**
     * Append reverse URN NSS paths to key.
     *
     * <p>
     * 101062745/v00001/content/data/400094393.jp2
     * to
     * 5472/6010/101062745/v00001/content/data/400094393.jp2
     * </p>
     *
     * @param key object key
     * @return remediated object key
     * @throws NumberFormatException not a number
     */
    String mapKey(String key) throws NumberFormatException {
        boolean renameVerified = verifyRename(key);

        if (renameVerified || verifyOnly) {
            return key;
        }

        // parse root "folder" from object key
        String nss = key.contains(PATH_SEPARATOR)
            ? key.substring(0, key.indexOf(PATH_SEPARATOR))
            : key;

        // ensure nss is a number
        Long.parseLong(nss);

        String reversedNss = reverse(leftPad(nss, 8, "0"));

        return format(
            "%s/%s/%s",
            reversedNss.substring(0, 4),
            reversedNss.substring(4, 8),
            key
        );
    }

    /**
     * Perform multiple checks to determine if key is expected rename format.
     *
     * @param key s3 object key
     * @return whether the key has already been renamed according to spec
     */
    boolean verifyRename(String key) {
        boolean verified = true;
        String[] path = key.split("/");
        if (path.length < 3) {
            // less than expected number of folders
            verified = false;
        }
        if (verified && !isNumeric(path[0]) && path[0].length() != 4) {
            // 1rd folder is not 4 digit number
            verified = false;
        }
        if (verified && !isNumeric(path[1]) && path[1].length() != 4) {
            // 2nd folder is not 4 digit number
            verified = false;
        }
        if (verified && !isNumeric(path[2])) {
            // 3rd folder is not number
            verified = false;
        }
        if (verified) {
            String reversedNss = reverse(leftPad(path[2], 8, "0"));
            verified = reversedNss.substring(0, 4).equals(path[0])
                && reversedNss.substring(4, 8).equals(path[1]);
        }

        return verified;
    }

}
