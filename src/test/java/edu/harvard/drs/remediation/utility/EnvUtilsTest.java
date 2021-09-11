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

import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.harvard.drs.remediation.utility.EnvUtils;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Environment utility tests.
 */
public class EnvUtilsTest {

    @Test
    public void testGet() throws Exception {
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            assertEquals(value, EnvUtils.get(key));
        }
    }

    @Test
    public void testGetParallelism() {
        assertEquals(12, EnvUtils.getParallelism());
    }

    @Test
    public void testGetInputPath() {
        assertEquals("./external/dump.txt", EnvUtils.getInputPath());
    }

    @Test
    public void testGetInputPattern() {
        assertEquals("^\\d+ : (\\d+) .*:(\\d+)$", EnvUtils.getInputPattern());
    }

    @Test
    public void testGetInputSkip() {
        assertEquals(2, EnvUtils.getInputSkip());
    }

    @Test
    public void testGetAwsBucketName() {
        assertEquals("harvard-drs-delivery", EnvUtils.getAwsBucketName());
    }

    @Test
    public void testGetAwsMaxKeys() {
        assertEquals(1000, EnvUtils.getAwsMaxKeys());
    }

    @Test
    public void testGetAwsMaxPartSize() {
        assertEquals(52428800L, EnvUtils.getAwsMaxPartSize());
    }

    @Test
    public void testGetAwsMultipartThreshold() {
        assertEquals(104857600L, EnvUtils.getAwsMultipartThreshold());
    }

    @Test
    public void testGetAwsSkipMultipart() {
        assertEquals(false, EnvUtils.getAwsSkipMultipart());
    }

}
