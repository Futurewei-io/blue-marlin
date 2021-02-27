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

import org.junit.Test;

import static org.junit.Assert.*;

public class TestIMSLogger
{

    @Test
    public void instance()
    {
        assertNotNull(IMSLogger.instance());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void info()
    {
        IMSLogger log = new IMSLogger();
        log.info("info");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testInfo()
    {
        IMSLogger log = new IMSLogger();
        log.info("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void error()
    {
        IMSLogger log = new IMSLogger();
        log.error("error");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testError()
    {
        IMSLogger log = new IMSLogger();
        log.error("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void debug()
    {
        IMSLogger log = new IMSLogger();
        log.debug("debug");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testDebug()
    {
        IMSLogger log = new IMSLogger();
        log.debug("String", 123);
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void warn()
    {
        IMSLogger log = new IMSLogger();
        log.warn("warn");
        assertNotNull(log);
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void testWarn()
    {
        IMSLogger log = new IMSLogger();
        log.warn("String", 123);
        assertNotNull(log);
    }
}