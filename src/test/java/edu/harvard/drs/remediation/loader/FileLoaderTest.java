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

package edu.harvard.drs.remediation.loader;

import static edu.harvard.drs.remediation.utility.EnvUtils.getInputPattern;
import static edu.harvard.drs.remediation.utility.EnvUtils.getInputSkip;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.harvard.drs.remediation.loader.FileLoader;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * File loader tests.
 */
public class FileLoaderTest {

    @Test
    public void testLoader() {
        final String inputPath = "src/test/resources/dump.txt";
        final String inputPattern = getInputPattern();
        final int skip = getInputSkip();

        final FileLoader loader = new FileLoader(inputPath, inputPattern, skip);

        List<SimpleEntry<String, String>> entries = loader.load()
            .collect(Collectors.toList());

        assertEquals("400171120", entries.get(0).getKey());
        assertEquals("12887296", entries.get(0).getValue());

        assertEquals("400171126", entries.get(1).getKey());
        assertEquals("12887299", entries.get(1).getValue());

        assertEquals("400171130", entries.get(2).getKey());
        assertEquals("12887301", entries.get(2).getValue());

        assertEquals("400171132", entries.get(3).getKey());
        assertEquals("12887302", entries.get(3).getValue());

        assertEquals("400171138", entries.get(4).getKey());
        assertEquals("12887305", entries.get(4).getValue());
    }

    @Test
    public void testLoaderFileNotFound() {
        final String inputPath = "src/test/resources/missing.txt";
        final String inputPattern = getInputPattern();
        final int skip = getInputSkip();

        final FileLoader loader = new FileLoader(inputPath, inputPattern, skip);

        assertThrows(RuntimeException.class, () -> {
            loader.load();
        });
    }

}
