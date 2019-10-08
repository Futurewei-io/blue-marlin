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

package com.bluemarlin.ims.imsservice.service;

import com.bluemarlin.ims.imsservice.exceptions.InputExceptions;
import com.bluemarlin.ims.imsservice.ml.IMSLinearRegression;
import com.bluemarlin.ims.imsservice.model.InventoryResult;
import com.bluemarlin.ims.imsservice.model.Range;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import org.json.JSONException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class UserEstimateService extends BaseService
{
    private static int NUMBER_OF_DAYS_FOR_LR_PREDICTION = 7;
    private static int NUMBER_OF_DAYS_FOR_AVERAGE = 30;
    private static ExecutorService executor = Executors.newCachedThreadPool();

    private List<String> getDaysForPrediction(Date today, int timeLength)
    {

        List<String> days = new ArrayList<>();

        /**
         * 1. Get today
         * 2. Go back 7 days plus timeLength in past and return all first 7 days
         */

        Calendar cal = Calendar.getInstance();
        cal.setTime(today);
        cal.add(Calendar.DATE, (-1) * (NUMBER_OF_DAYS_FOR_AVERAGE + timeLength - 1));
        for (int i = 0; i < NUMBER_OF_DAYS_FOR_AVERAGE; i++)
        {
            String day = CommonUtil.DateToDayString(cal.getTime());
            days.add(day);
            cal.add(Calendar.DATE, 1);
        }
        return days;
    }

    private List<String> getNConsequenceDays(String day, int n) throws ParseException
    {
        List<String> result = new ArrayList<>();
        result.add(day);
        if (n > 1)
        {
            Calendar cal = Calendar.getInstance();
            Date date = CommonUtil.dayToDate(day);
            cal.setTime(date);
            for (int i = 1; i < n; i++)
            {
                cal.add(Calendar.DATE, 1);
                day = CommonUtil.DateToDayString(cal.getTime());
                result.add(day);
            }
        }
        return result;
    }

    private int getUserEstimate(TargetingChannel targetingChannel, Range range, double price, Date today) throws ParseException, IOException, JSONException, InputExceptions, ExecutionException, InterruptedException
    {
        /**
         * Grabbing the length of the range.
         */
        int timeLength = range.getLength();
        if (timeLength < 0)
        {
            throw new InputExceptions("Wrong input days st > ed!");
        }

        /**
         * Getting the time distance between today and st.
         */
        int timeDistance = Range.getLength(CommonUtil.DateToDayString(today), range.getSt());
        if (timeDistance < 0)
        {
            throw new InputExceptions("Wrong input days today > st!");
        }

        /**
         *Figuring out all the days in past that we need.
         */
        List<String> days = getDaysForPrediction(today, timeLength);
        List<Long> countsForEachTimePeriod = new ArrayList<>();
        Map<String, Future<Long>> futureMap = new HashMap<>();
        for (String day : days)
        {
            List<String> daysUsedForQuery = getNConsequenceDays(day, timeLength);

            /**
             * Making ES calls to grab distinct users for these days.
             */
            Future future = executor.submit((Callable<Object>) () ->
            {
                long counts = userEstimateDao.getUserCount(targetingChannel, daysUsedForQuery, price);
                return counts;
            });
            futureMap.put(day, future);
        }

        for (String day : days)
        {
            long count = futureMap.get(day).get();
            countsForEachTimePeriod.add(count);
        }

        /**
         * Performing average and lr until we get to start date
         *
         */
        double lr = 0, ave = 0, result = 0;
        for (int i = 0; i < timeDistance; i++)
        {
            lr = IMSLinearRegression.predict(countsForEachTimePeriod, NUMBER_OF_DAYS_FOR_LR_PREDICTION);
            ave = IMSLinearRegression.average(countsForEachTimePeriod, NUMBER_OF_DAYS_FOR_AVERAGE);
            result = (lr + ave) / 2;
            countsForEachTimePeriod.add((long) result);
        }

        return (int) result;
    }

    /**
     * This service returns number of unique users that belong to a targeting channel in a period of time.
     *
     * @param targetingChannel
     * @param ranges
     * @param price
     * @param today
     * @return
     * @throws ParseException
     * @throws IOException
     * @throws JSONException
     * @throws InputExceptions
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public InventoryResult getUserEstimate(TargetingChannel targetingChannel, List<Range> ranges, double price, Date today) throws ParseException, IOException, JSONException, InputExceptions, ExecutionException, InterruptedException
    {
        /**
         * Making one call for different ranges.
         */
        long count = 0;
        for (Range range : ranges)
        {
            count += getUserEstimate(targetingChannel, range, price, today);
        }
        InventoryResult result = new InventoryResult(count);
        return result;
    }

}
