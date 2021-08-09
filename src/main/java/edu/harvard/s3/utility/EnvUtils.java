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

package edu.harvard.s3.utility;

import static java.lang.Integer.parseInt;
import static java.util.Optional.ofNullable;

import java.util.Map;
import java.util.Optional;

/**
 * Environment utility for access to environment variables with defaults.
 */
public final class EnvUtils {

    static final String PARALLELISM = "PARALLELISM";

    static final String INPUT_PATH = "INPUT_PATH";
    static final String INPUT_PATTERN = "INPUT_PATTERN";
    static final String INPUT_SKIP = "INPUT_SKIP";

    static final String AWS_BUCKET_NAME = "AWS_BUCKET_NAME";
    static final String AWS_MAX_KEYS = "AWS_MAX_KEYS";

    private static final Map<String, String> DEFAULT_ENV = Map.of(
        PARALLELISM, "12",
        INPUT_PATH, "./external/dump.txt",
        INPUT_PATTERN, "^\\d+ : (\\d+) .*:(\\d+)$",
        INPUT_SKIP, "2",
        AWS_BUCKET_NAME, "delivery",
        AWS_MAX_KEYS, "5000"
    );

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
     * Retrieve environment AWS bucket name. (default delivery)
     *
     * @return AWS bucket name
     */
    public static String getAwsBucketName() {
        return get(AWS_BUCKET_NAME);
    }

    /**
     * Retrieve environment AWS max keys. (default 5000)
     *
     * @return AWS max keys
     */
    public static int getAwsMaxKeys() {
        return parseInt(get(AWS_MAX_KEYS));
    }

    static String get(String key) {
        Optional<String> var = ofNullable(System.getenv(key));
        if (var.isPresent()) {
            return var.get();
        }

        return DEFAULT_ENV.get(key);
    }

}
