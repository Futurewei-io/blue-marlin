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
import org.apache.bluemarlin.ims.imsservice.exceptions.FailedLockingException;
import org.apache.bluemarlin.ims.imsservice.exceptions.NoInventoryExceptions;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class TestPerformance extends TestBookingService
{
    public TestPerformance() throws IOException
    {
    }

    @Test
    public void createBookings() throws IOException, JSONException, InterruptedException, FailedLockingException, ESConnectionException, NoInventoryExceptions, ExecutionException
    {
        clearBookings();

        /**
         * Create 14 bookings
         */

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
        tc.setSi(Arrays.asList("2","4"));
        tc.setPdas(Arrays.asList("365", "388"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2","4"));
        tc.setM(Arrays.asList("m2"));
        tc.setPdas(Arrays.asList("253","365", "388"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("4"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc.setSi(Arrays.asList("2","4"));
        tc.setM(Arrays.asList("m2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("253","365", "388"));
        bookAndVerify(tc, 10);

        tc.setG(Arrays.asList("g_f"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("2"));
        tc.setPdas(Arrays.asList("254"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("254", "266"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("2","4"));
        tc.setPdas(Arrays.asList("365", "388"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("2","4"));
        tc.setM(Arrays.asList("m2"));
        tc.setPdas(Arrays.asList("253","365", "388"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("4"));
        bookAndVerify(tc, 10);

        tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setSi(Arrays.asList("2","4"));
        tc.setM(Arrays.asList("m2"));
        tc.setDms(Arrays.asList("rneal00", "bndal00"));
        tc.setPdas(Arrays.asList("253","365", "388"));
        bookAndVerify(tc, 10);
    }
}

