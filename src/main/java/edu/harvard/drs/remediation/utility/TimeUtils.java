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

import static java.lang.System.nanoTime;

/**
 * Time utility to compute time in milliseconds.
 */
public final class TimeUtils {

    private TimeUtils() { }

    /**
     * Compute time elapsed from start time in milliseconds.
     *
     * @return time elapsed
     */
    public static double elapsed(long startTime) {
        return (nanoTime() - startTime) / 1000000.0;
    }

}
