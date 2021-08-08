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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * In memory map tests.
 */
public class InMemoryMapTest {

    @Test
    public void testInMemoryMap() {
        String key = "foo";
        String value = "bar";

        assertEquals(0, InMemoryMap.size());

        InMemoryMap.put(key, value);

        assertEquals(value, InMemoryMap.get(key));

        assertEquals(1, InMemoryMap.size());

        InMemoryMap.clear();

        assertEquals(0, InMemoryMap.size());
    }

}
