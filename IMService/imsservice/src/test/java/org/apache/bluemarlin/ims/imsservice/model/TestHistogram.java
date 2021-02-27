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

package org.apache.bluemarlin.ims.imsservice.model;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestHistogram
{

    @Test
    public void getH()
    {
        Histogram hstm = new Histogram();
        assertNull(hstm.getH());
        hstm.setH("2018-01-05");
        assertEquals("2018-01-05", hstm.getH());
    }

    @Test
    public void setH()
    {
        Histogram hstm = new Histogram();
        hstm.setH("2018-01-05");
        assertEquals("2018-01-05", hstm.getH());
    }

    @Test
    public void getT()
    {
        Histogram hstm = new Histogram();
        assertNull(hstm.getT());
        hstm.setT((long) 123456);
        assertEquals(123456L, hstm.getT(), 0);
    }

    @Test
    public void setT()
    {
        Histogram hstm = new Histogram();
        hstm.setT((long) 123456);
        assertEquals(123456L, hstm.getT(), 0);
    }
}