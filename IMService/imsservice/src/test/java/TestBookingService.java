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

import org.apache.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import org.apache.bluemarlin.ims.imsservice.exceptions.FailedLockingException;
import org.apache.bluemarlin.ims.imsservice.exceptions.NoInventoryExceptions;
import org.apache.bluemarlin.ims.imsservice.model.BookResult;
import org.apache.bluemarlin.ims.imsservice.model.InventoryResult;
import org.apache.bluemarlin.ims.imsservice.model.Range;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.fail;

public class TestBookingService extends TestBase
{
    static double DEFAULT_PRICE = 0;
    static List<Range> DEFAULT_RANGE_1;
    static String DEFAULT_ADV_ID = "advid100";
    static double MAX_THRESHOLD = 3;
    static long DEFAULT_WAIT_AFTER_BOOKING_ms = 2000;

    static
    {
        DEFAULT_RANGE_1 = new ArrayList<>();
        Range r1 = new Range();
        r1.setSt("2018-11-07");
        r1.setEd("2018-11-07");
        r1.setSh("0");
        r1.setEh("23");
        DEFAULT_RANGE_1.add(r1);
    }

    public TestBookingService() throws IOException
    {
    }

    private double getInventoryEstimate(TargetingChannel targetingChannel, List<Range> ranges) throws ESConnectionException, JSONException, IOException, ExecutionException, InterruptedException
    {
        InventoryResult result = inventoryEstimateService.aggregateInventory(targetingChannel, ranges, TestBookingService.DEFAULT_PRICE);
        return result.getAvailCount();
    }

    protected void bookAndVerify(TargetingChannel tc, double percentageOfInventory) throws ESConnectionException, JSONException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        double inventory = getInventoryEstimate(tc, DEFAULT_RANGE_1);

        long toBook = (long) (inventory * percentageOfInventory / 100.0);
        BookResult bookResult = bookingService.book(tc, DEFAULT_RANGE_1, DEFAULT_PRICE, DEFAULT_ADV_ID, toBook);
        long booked = bookResult.getTotalBooked();

        Thread.sleep(DEFAULT_WAIT_AFTER_BOOKING_ms);

        double inventoryAfter = getInventoryEstimate(tc, DEFAULT_RANGE_1);

        if (Math.abs(booked - toBook) > MAX_THRESHOLD && booked > 0)
        {
            fail(String.format("toBook:%07d, booked:%07d", toBook, booked));
        }

        /**
         * The reported inventory after might be lot less than inventory - booked
         * because the partially filled bucket is considered fully booked
         */
        if (inventory - booked < inventoryAfter)
        {
            fail(String.format("inventory - inventoryAfter - booked:%07d",
                    (long) (inventory - inventoryAfter - booked)));
        }
    }

    public void clearBookings() throws IOException, JSONException
    {
        super.clearBookings();
    }

    @Test
    public void test11() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));

        bookAndVerify(tc, 1);
    }

    @Test
    public void test12() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 50);
        bookAndVerify(tc, 90);
        bookAndVerify(tc, 50);
    }

    @Test
    public void test13() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setDms(Arrays.asList("rneal00"));

        bookAndVerify(tc, 50);
        bookAndVerify(tc, 100);
    }

    @Test
    public void test21() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test22() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("1"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test31() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test32() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m", "g_f"));
        tc.setSi(Arrays.asList("1", "2"));
        tc.setPdas(Arrays.asList("266", "254"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test41() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("1", "2"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test42() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("266"));

        bookAndVerify(tc, 50);
    }

    @Test
    public void test51() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);
    }

    @Test
    public void test52() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));

        bookAndVerify(tc, 50);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));

        bookAndVerify(tc, 90);
    }

    @Test
    public void test53() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        bookAndVerify(tc, 95);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));
        bookAndVerify(tc, 50);
    }

    @Test
    public void test61() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));
        bookAndVerify(tc, 10);
    }

    @Test
    public void test62() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        clearBookings();

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2", "4"));
        tc.setPdas(Arrays.asList("365", "388"));
        bookAndVerify(tc, 10);
    }

    @Test
    public void testQuery() throws JSONException, ESConnectionException, IOException, InterruptedException, NoInventoryExceptions, FailedLockingException, ExecutionException
    {
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));

        double inventory = getInventoryEstimate(tc, DEFAULT_RANGE_1);
        System.out.println(inventory);
    }

}
