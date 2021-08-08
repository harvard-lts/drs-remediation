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

import static java.lang.Runtime.getRuntime;

/**
 * Runtime utility for getting runtime details.
 */
public final class RuntimeUtils {

    private static final Runtime RUNTIME = getRuntime();

    private RuntimeUtils() { }

    /**
     * Lookup runtime available processors.
     *
     * @return runtime available processors
     */
    public static int availableProcessors() {
        return RUNTIME.availableProcessors();
    }

    /**
     * Lookup runtime total memory in GiB.
     *
     * @return runtime total memory in GiB
     */
    public static double totalMemory() {
        return RUNTIME.totalMemory() / (1024.0 * 1024.0 * 1024.0);
    }

}
