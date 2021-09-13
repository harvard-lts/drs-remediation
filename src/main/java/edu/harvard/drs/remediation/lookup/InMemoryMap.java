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

package edu.harvard.drs.remediation.lookup;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In memory map for thread-safe operations.
 */
public final class InMemoryMap {

    private static final Map<String, String> MAP = new ConcurrentHashMap<>();

    private InMemoryMap() { }

    /**
     * Put key value pair into map.
     *
     * @param key   lookup key
     * @param value value for key
     */
    public static void put(String key, String value) {
        MAP.put(key, value);
    }

    /**
     * Get value for key.
     *
     * @param key lookup key
     * @return value for key
     */
    public static String get(String key) {
        return MAP.get(key);
    }

    /**
     * Get size of the in memory map.
     *
     * @return size of map
     */
    public static int size() {
        return MAP.size();
    }

    /**
     * Clear in memory map.
     */
    public static void clear() {
        MAP.clear();
    }

}
