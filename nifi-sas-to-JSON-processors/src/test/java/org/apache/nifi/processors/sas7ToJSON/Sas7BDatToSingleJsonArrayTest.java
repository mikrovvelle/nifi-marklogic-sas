/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.sas7ToJSON;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

public class Sas7BDatToSingleJsonArrayTest {

    private TestRunner testRunner;
    private ClassLoader classLoader = this.getClass().getClassLoader();
    private InputStream arns7b = new FileInputStream(
            Objects.requireNonNull(classLoader.getResource("all_rand_normal.sas7bdat")).getFile());
    private InputStream arns7b2 = new FileInputStream(
            Objects.requireNonNull(classLoader.getResource("all_rand_normal.sas7bdat")).getFile());
    private InputStream blank = new FileInputStream(
            Objects.requireNonNull(classLoader.getResource("blank.sas7bdat")).getFile());

    private static final Relationship SUCCESS = Sas7BDatToSingleJsonArray.SUCCESS;
    private static final Relationship FAILURE = Sas7BDatToSingleJsonArray.FAILURE;

    public Sas7BDatToSingleJsonArrayTest() throws FileNotFoundException {
    }

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(Sas7BDatToSingleJsonArray.class);
    }

    @Test
    public void testProcessor() {

        TestRunner runner = TestRunners.newTestRunner(new Sas7BDatToSingleJsonArray());
        runner.enqueue(arns7b);
        runner.enqueue(arns7b2);

        runner.assertQueueNotEmpty();
        runner.run(2);
        runner.assertQueueEmpty();

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(Sas7BDatToSingleJsonArray.SUCCESS);
        String resultValue = new String(runner.getContentAsByteArray(results.get(0)));
        assertEquals(2, runner.getFlowFilesForRelationship(SUCCESS).size());
    }

    @Test
    public void testEmptyProcessing() {
        TestRunner runner = TestRunners.newTestRunner(Sas7BDatToSingleJsonArray.class);
        runner.enqueue(blank);

        runner.assertQueueNotEmpty();
        runner.run(1);
        runner.assertQueueEmpty();
        assertTrue(
                runner.getFlowFilesForRelationship(SUCCESS).isEmpty());
        assertEquals(1,
                runner.getFlowFilesForRelationship(FAILURE).size());

    }
}
