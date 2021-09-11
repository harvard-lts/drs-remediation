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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleEntry;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;

/**
 * File loader for reading a file and processing each line with a pattern and
 * put into a stream of simple entries.
 */
@Slf4j
public class FileLoader implements Loader<String, String> {

    private final Path path;

    private final Pattern pattern;

    private final int skip;

    /**
     * File loader constructor.
     *
     * @param path    path to file to load
     * @param pattern pattern to process each line of file
     * @param skip    number of lines of file to skip before processing
     */
    public FileLoader(String path, String pattern, int skip) {
        this.path = Paths.get(path);
        this.pattern = Pattern.compile(pattern);
        this.skip = skip;
    }

    @Override
    public Stream<SimpleEntry<String, String>> load() {
        log.info("loading from file '{}' with pattern '{}' skipping {} lines", path, pattern, skip);
        try {
            return Files.lines(path)
                .skip(skip)
                .map(l -> pattern.matcher(l))
                .filter(m -> m.matches())
                .map(m -> new SimpleEntry<String, String>(m.group(1), m.group(2)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load file", e);
        }
    }

}
