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

import static edu.harvard.drs.remediation.utility.EnvUtils.getInputPattern;
import static edu.harvard.drs.remediation.utility.EnvUtils.getInputSkip;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.harvard.drs.remediation.loader.FileLoader;
import edu.harvard.drs.remediation.lookup.InMemoryLookupTable;
import org.junit.jupiter.api.Test;

/**
 * In memory lookup table tests.
 */
public class InMemoryLookupTableTest {

    @Test
    public void testLookupTable() {
        final String inputPath = "src/test/resources/dump.txt";
        final String inputPattern = getInputPattern();
        final int skip = getInputSkip();

        final FileLoader loader = new FileLoader(inputPath, inputPattern, skip);

        final InMemoryLookupTable lookupTable = new InMemoryLookupTable(loader);

        assertEquals(0, lookupTable.size());

        lookupTable.load();

        assertEquals(5, lookupTable.size());

        assertEquals("12887296", lookupTable.get("400171120"));
        assertEquals("12887299", lookupTable.get("400171126"));
        assertEquals("12887301", lookupTable.get("400171130"));
        assertEquals("12887302", lookupTable.get("400171132"));
        assertEquals("12887305", lookupTable.get("400171138"));

        lookupTable.set("foo", "bar");

        assertEquals("bar", lookupTable.get("foo"));

        lookupTable.unload();

        assertEquals(0, lookupTable.size());
    }

}
