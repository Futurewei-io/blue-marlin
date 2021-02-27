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

public class TestBookResult
{

    @Test
    public void setTotalBooked()
    {
        Long bkQty = (long) 10, setQty = (long) 20;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        bkres1.setTotalBooked(setQty);

        assertNotNull(bkres1);
        assertEquals(setQty, bkres1.getTotalBooked());
    }

    @Test
    public void getBookId()
    {
        Long bkQty = (long) 10;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        assertEquals("test_bkid_1", bkres1.getBookId());
    }

    @Test
    public void getTotalBooked()
    {
        Long bkQty = (long) 10;
        BookResult bkres1 = new BookResult("test_bkid_1", bkQty);
        assertEquals(bkQty, bkres1.getTotalBooked());
    }
}