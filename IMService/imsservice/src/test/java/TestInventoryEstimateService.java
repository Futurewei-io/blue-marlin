/**
 * Copyright 2019, Futurewei Technologies
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import org.apache.bluemarlin.ims.imsservice.model.InventoryResult;
import org.apache.bluemarlin.ims.imsservice.model.Range;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

public class TestInventoryEstimateService extends TestBase
{

    public TestInventoryEstimateService() throws IOException
    {
    }

    @Before
    public void init() throws IOException
    {
    }

    public void testInventoryEstimate(TargetingChannel tc, List<Range> ranges, double price, long actual, int tolerance) throws JSONException, ESConnectionException, IOException, ExecutionException, InterruptedException
    {
        InventoryResult result = inventoryEstimateService.aggregateInventory(tc, ranges, price);
        if (result.getAvailCount() > actual + tolerance || result.getAvailCount() < actual - tolerance)
        {
            fail(String.format("return = %d, actual = %d", result.getAvailCount(), actual));
        }
    }

    public void observeInventoryEstimate(TargetingChannel tc, List<Range> ranges, double price) throws ESConnectionException, JSONException, IOException, ExecutionException, InterruptedException
    {
        InventoryResult result = inventoryEstimateService.aggregateInventory(tc, ranges, price);
        System.out.println(String.format("return = %d", result.getAvailCount()));
    }

    protected long getActualValue(int numberOfReturnedDocs, int hLevel, int numberOfTimePoints)
    {
        long result = 0;
        if (hLevel == 0)
        {
            hLevel = 1;
        }
        else
        {
            hLevel = hLevel * 100;
        }
        result = hLevel * numberOfReturnedDocs * numberOfTimePoints;
        return result;
    }

    protected long getActualValue(int numberOfReturnedDocs, int hLevel, int numberOfTimePoints, double ratio)
    {
        long result = (long) (getActualValue(numberOfReturnedDocs, hLevel, numberOfTimePoints) * ratio);
        return result;
    }

    @Test
    public void test11() throws JSONException, ESConnectionException, IOException, ExecutionException, InterruptedException
    {
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

        observeInventoryEstimate(tc, ranges, price);
    }

    @Test
    public void test21() throws JSONException, ESConnectionException, IOException, ExecutionException, InterruptedException
    {
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        double price = 0;

        List<Range> ranges = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-11-07");
        r1.setEd("2018-11-07");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        observeInventoryEstimate(tc, ranges, price);
    }



}
