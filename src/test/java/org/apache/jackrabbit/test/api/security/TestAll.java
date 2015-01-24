/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.test.api.security;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * <code>TestAll</code>...
 */
/**
 * Test suite
 */
public class TestAll extends TestCase {

    /**
     * Returns a <code>Test</code> suite that executes all tests inside this
     * package.
     *
     * @return a <code>Test</code> suite that executes all tests inside this
     *         package.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("jsr 283 security tests");

        suite.addTestSuite(AccessControlDiscoveryTest.class);
        suite.addTestSuite(AccessControlPolicyTest.class);
        suite.addTestSuite(AccessControlPolicyIteratorTest.class);
        suite.addTestSuite(AccessControlListTest.class);

        // tests with read only session:
        suite.addTestSuite(RSessionAccessControlDiscoveryTest.class);
        suite.addTestSuite(RSessionAccessControlPolicyTest.class);
        suite.addTestSuite(RSessionAccessControlTest.class);

        return suite;
    }
}
