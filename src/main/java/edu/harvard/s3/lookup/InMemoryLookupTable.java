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

package edu.harvard.s3.lookup;

import static edu.harvard.s3.utility.RuntimeUtils.totalMemory;
import static edu.harvard.s3.utility.TimeUtils.elapsed;

import edu.harvard.s3.loader.Loader;
import lombok.extern.slf4j.Slf4j;

/**
 * In memory lookup table implementation.
 */
@Slf4j
public class InMemoryLookupTable implements LookupTable<String, String> {

    private final Loader<String, String> loader;

    /**
     * In memory lookup table constructor.
     *
     * @param loader loader in which to load lookup table
     */
    public InMemoryLookupTable(Loader<String, String> loader) {
        this.loader = loader;
    }

    @Override
    public void load() {
        long startTime = System.nanoTime();

        loader.load()
            .forEach(e -> set(e.getKey(), e.getValue()));

        log.info("{} key value pairs loaded into memory", InMemoryMap.size());
        log.debug("{} milliseconds to load in memory", elapsed(startTime));
        log.debug("{} GiB total memory used after loading lookup table", totalMemory());
    }

    @Override
    public void set(String key, String value) {
        InMemoryMap.put(key, value);
    }

    @Override
    public String get(String key) {
        return InMemoryMap.get(key);
    }

    @Override
    public int size() {
        return InMemoryMap.size();
    }

    @Override
    public void unload() {
        InMemoryMap.clear();
    }

}
