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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class TestIMSBookingRequest
{

    @Test
    public void getTargetingChannel()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        double price = 0;
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        bkReq.setTargetingChannel(tc);

        assertNotNull(bkReq.getTargetingChannel());
        assertEquals(tc, bkReq.getTargetingChannel());
    }

    @Test
    public void setTargetingChannel()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        double price = 0;
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-11-07");
        r1.setEd("2018-11-07");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        bkReq.setTargetingChannel(tc);

        assertNotNull(bkReq.getTargetingChannel());
        assertEquals(tc, bkReq.getTargetingChannel());
    }

    @Test
    public void getPrice()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setPrice(10000.1);
        assertEquals(10000.1, bkReq.getPrice(), 0);
    }

    @Test
    public void setPrice()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setPrice(1.1);
        assertEquals(1.1, bkReq.getPrice(), 0);
    }

    @Test
    public void getDays()
    {
        List<Range> ranges = new ArrayList<>();
        Range r3 = new Range();
        r3.setSt("2018-01-07");
        r3.setEd("2018-01-07");
        r3.setSh("0");
        r3.setEh("23");
        ranges.add(r3);
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setDays(ranges);
        assertEquals(ranges, bkReq.getDays());
    }

    @Test
    public void setDays()
    {
        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-01-05");
        r1.setEd("2018-01-05");
        r1.setSh("0");
        r1.setEh("23");
        Range r2 = new Range();
        r2.setSt("2018-01-06");
        r2.setEd("2018-01-06");
        r2.setSh("0");
        r2.setEh("23");
        Range r3 = new Range();
        r3.setSt("2018-01-07");
        r3.setEd("2018-01-07");
        r3.setSh("0");
        r3.setEh("23");
        ranges.add(r1);
        ranges.add(r2);
        ranges.add(r3);
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setDays(ranges);
        assertEquals(ranges, bkReq.getDays());
    }

    @Test
    public void getRequestCount()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setRequestCount(2000);
        assertEquals(2000, bkReq.getRequestCount(), 0);
    }

    @Test
    public void setRequestCount()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setRequestCount(1000);
        assertEquals(1000, bkReq.getRequestCount(), 0);
    }

    @Test
    public void getAdvID()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setAdvID("test_adv_id_200");
        assertEquals("test_adv_id_200", bkReq.getAdvID());
    }

    @Test
    public void setAdvID()
    {
        IMSBookingRequest bkReq = new IMSBookingRequest();
        bkReq.setAdvID("test_adv_id_100");
        assertEquals("test_adv_id_100", bkReq.getAdvID());
    }
}