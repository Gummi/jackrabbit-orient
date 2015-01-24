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
package org.apache.jackrabbit.test.api.query;

import java.util.List;
import java.util.Arrays;

import javax.jcr.RepositoryException;
import javax.jcr.query.InvalidQueryException;

/**
 * <code>CreateQueryTest</code> checks if {@link javax.jcr.query.QueryManager#createQuery(String, String)}
 * throws an {@link javax.jcr.query.InvalidQueryException} for an unknown query language.
 */
public class CreateQueryTest extends AbstractQueryTest {

    public void testUnknownQueryLanguage() throws RepositoryException {
        List supported = Arrays.asList(qm.getSupportedQueryLanguages());
        String language;
        do {
            language = createRandomString(5);
        } while (supported.contains(language));
        try {
            qm.createQuery("foo", language);
            fail("createQuery() must throw for unknown query language: " + language);
        } catch (InvalidQueryException e) {
            // expected
        }
    }
}
