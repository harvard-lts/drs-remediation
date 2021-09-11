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

/**
 * Lookup table interface.
 */
public interface LookupTable<K, V> {

    /**
     * Load lookup map.
     */
    void load();

    /**
     * Add key value pair in lookup map.
     *
     * @param key   lookup key
     * @param value value for key
     */
    void set(K key, V value);

    /**
     * Retrieve value for key.
     *
     * @param key lookup key
     * @return value at key
     */
    V get(K key);

    /**
     * Size of lookup table.
     *
     * @return size of lookup table
     */
    int size();

    /**
     * Unload lookup table.
     */
    void unload();

}
