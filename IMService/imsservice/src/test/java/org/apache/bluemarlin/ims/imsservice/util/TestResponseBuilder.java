/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0.html
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.bluemarlin.ims.imsservice.util;

import org.apache.bluemarlin.ims.imsservice.exceptions.IMSException;
import org.junit.Test;
import org.springframework.http.ResponseEntity;

import static org.junit.Assert.assertNotNull;

public class TestResponseBuilder
{

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void build()
    {
        ResponseEntity res = ResponseBuilder.build(new Object(), 100);
        assertNotNull(res);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void buildError()
    {
        Exception exc = new Exception();
        ResponseEntity res = ResponseBuilder.buildError(exc);
        assertNotNull(res);
        exc = new IMSException("imserr");
        res = ResponseBuilder.buildError(exc);
        assertNotNull(res);
    }
}