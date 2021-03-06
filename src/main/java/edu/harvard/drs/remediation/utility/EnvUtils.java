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

package edu.harvard.drs.remediation.utility;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Optional.ofNullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Environment utility for access to environment variables with defaults.
 */
public final class EnvUtils {

    static final String PARALLELISM = "PARALLELISM";

    static final String VERIFY_ONLY = "VERIFY_ONLY";

    static final String INPUT_PATH = "INPUT_PATH";
    static final String INPUT_PATTERN = "INPUT_PATTERN";
    static final String INPUT_SKIP = "INPUT_SKIP";

    static final String AWS_BUCKET_NAME = "AWS_BUCKET_NAME";
    static final String AWS_ENDPOINT_OVERRIDE = "AWS_ENDPOINT_OVERRIDE";

    static final String AWS_MAX_KEYS = "AWS_MAX_KEYS";
    static final String AWS_MAX_PART_SIZE = "AWS_MAX_PART_SIZE";
    static final String AWS_MULTIPART_THRESHOLD = "AWS_MULTIPART_THRESHOLD";
    static final String AWS_SKIP_MULTIPART = "AWS_SKIP_MULTIPART";

    private static final Map<String, String> DEFAULT_ENV = new HashMap<>() {
        {
            put(PARALLELISM, "12");
            put(VERIFY_ONLY, "false");
            put(INPUT_PATH, "./external/dump.txt");
            put(INPUT_PATTERN, "^\\d+ : (\\d+) .*:(\\d+)$");
            put(INPUT_SKIP, "2");
            put(AWS_BUCKET_NAME, "harvard-drs-delivery");
            put(AWS_ENDPOINT_OVERRIDE, "");
            put(AWS_MAX_KEYS, "1000");
            put(AWS_MAX_PART_SIZE, "52428800");
            put(AWS_MULTIPART_THRESHOLD, "104857600");
            put(AWS_SKIP_MULTIPART, "false");
        }
    };

    private EnvUtils() { }

    /**
     * Retrieve environment parallelism. (default 12)
     *
     * @return parallelism
     */
    public static int getParallelism() {
        return parseInt(get(PARALLELISM));
    }

    /**
     * Retrieve environment verify onlt. (default false)
     *
     * @return whther to verify only
     */
    public static boolean getVerifyOnly() {
        return parseBoolean(get(VERIFY_ONLY));
    }

    /**
     * Retrieve environment input file path. (default ./external/dump.txt)
     *
     * @return input path
     */
    public static String getInputPath() {
        return get(INPUT_PATH);
    }

    /**
     * Retrieve environment pattern for the input file processing. (default ^\\d+ : (\\d+) .*:(\\d+)$)
     *
     * @returninput pattern
     */
    public static String getInputPattern() {
        return get(INPUT_PATTERN);
    }

    /**
     * Retrieve environment number of lines to skip of the input file. (default 2)
     *
     * @return input skip
     */
    public static int getInputSkip() {
        return parseInt(get(INPUT_SKIP));
    }

    /**
     * Retrieve environment AWS bucket name. (default harvard-drs-delivery)
     *
     * @return AWS bucket name
     */
    public static String getAwsBucketName() {
        return get(AWS_BUCKET_NAME);
    }

    /**
     * Retrieve environment AWS endpoint override. (default ``)
     *
     * @return AWS endpoint override
     */
    public static String getAwsEndpointOverride() {
        return get(AWS_ENDPOINT_OVERRIDE);
    }

    /**
     * Retrieve environment AWS max keys. (default 1000)
     *
     * @return AWS max keys
     */
    public static int getAwsMaxKeys() {
        return parseInt(get(AWS_MAX_KEYS));
    }

    /**
     * Retrieve environment AWS max part size. (default 52428800 = 50 MiB)
     *
     * @return AWS max part size
     */
    public static long getAwsMaxPartSize() {
        return parseLong(get(AWS_MAX_PART_SIZE));
    }

    /**
     * Retrieve environment AWS multipart threshold. (default 104857600 = 100 MiB)
     *
     * @return AWS multipart threshold
     */
    public static long getAwsMultipartThreshold() {
        return parseLong(get(AWS_MULTIPART_THRESHOLD));
    }

    /**
     * Retrieve environment AWS skip multipart. (default false)
     *
     * @return AWS skip multipart
     */
    public static boolean getAwsSkipMultipart() {
        return parseBoolean(get(AWS_SKIP_MULTIPART));
    }

    static String get(String key) {
        Optional<String> var = ofNullable(System.getenv(key));
        if (var.isPresent()) {
            return var.get();
        }

        return DEFAULT_ENV.get(key);
    }

}
