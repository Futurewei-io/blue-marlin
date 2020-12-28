/**
 * Copyright 2019, Futurewei Technologies
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bluemarlin.ims.imsservice.service;

import com.bluemarlin.ims.imsservice.dao.booking.BookingDao;
import com.bluemarlin.ims.imsservice.dao.inventory.InventoryEstimateDao;
import com.bluemarlin.ims.imsservice.dao.tbr.TBRDao;
import com.bluemarlin.ims.imsservice.model.Booking;
import com.bluemarlin.ims.imsservice.model.BookingBucket;
import com.bluemarlin.ims.imsservice.model.Day;
import com.bluemarlin.ims.imsservice.model.Impression;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import org.json.JSONException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

public class BookingBucketAllocation implements Callable
{

    public static class UtilVariables
    {
        private List<BookingBucket> added;
        private List<BookingBucket> removed;
        private AtomicLong atomicRequestAmount;
        private AtomicLong atomicBookedAmount;

        public UtilVariables(List<BookingBucket> added, List<BookingBucket> removed, AtomicLong atomicRequestAmount, AtomicLong atomicBookedAmount)
        {
            this.atomicBookedAmount = atomicBookedAmount;
            this.atomicRequestAmount = atomicRequestAmount;
            this.added = added;
            this.removed = removed;
        }
    }

    private InventoryEstimateService inventoryEstimateService;
    private BookingDao bookingDao;
    private InventoryEstimateDao inventoryEstimateDao;
    private TBRDao tbrDao;
    private Booking booking;
    private TargetingChannel targetingChannel;
    private Day day;
    private BookingBucket bookingBucket;
    private Map<String, Booking> bookingsMapForDay;
    private UtilVariables utilVariables;


    private long calculateBookingAmount(long potentialBooking, AtomicLong atomicRequestAmount, AtomicLong atomicBookedAmount)
    {
        synchronized (this.inventoryEstimateService)
        {
            long amountBookedForThisBB = 0;

            if (potentialBooking <= 0)
            {
                return 0;
            }

            if (potentialBooking < atomicRequestAmount.get())
            {
                amountBookedForThisBB = potentialBooking;
            }
            else
            {
                amountBookedForThisBB = atomicRequestAmount.get();
            }
            atomicBookedAmount.addAndGet(amountBookedForThisBB);
            atomicRequestAmount.addAndGet(-1 * amountBookedForThisBB);
            return amountBookedForThisBB;
        }
    }

    /**
     * This method calculates the average TBR of a booking-bucket. The result is saved as
     * as part of booking-bucket object.
     *
     * @param day
     * @param BB
     * @param bookingsMap
     * @throws IOException
     * @throws JSONException
     */
    private void calculateAvgTBRMap(Day day, BookingBucket BB, Map<String, Booking> bookingsMap) throws IOException, JSONException
    {
        Map<String, List<Double>> result = new HashMap<>();
        List<TargetingChannel> Bns = inventoryEstimateService.extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, BB.getAndBookingsIds());
        List<String> bmIds = BB.getMinusBookingsIds();

        if (!CommonUtil.isEmpty(Bns))
        {
            for (String bkId : bmIds)
            {
                Booking bookingObj = bookingsMap.get(bkId);
                /**
                 * If booking is null means that the booking is deleted.
                 */
                if (bookingObj != null)
                {
                    TargetingChannel bm = bookingObj.getQuery();
                    List<Double> value = new ArrayList<>();
                    List<TargetingChannel> _tcs = new ArrayList<>();
                    _tcs.addAll(Bns);
                    _tcs.add(bm);
                    Impression impression = inventoryEstimateDao.getPredictions_PI_TCs(day, _tcs);
                    double tbr = tbrDao.getTBRRatio_PI_TCs(_tcs);
                    double impressionValue = impression.getTotal();
                    value.add(impressionValue);
                    value.add(tbr);
                    value.addAll(impression.getHs());
                    result.put(bkId, value);
                }
            }
        }

        BB.setAvgTbrMap(result);
    }

    private List<Map<String, Double>> splitBookedAmount(Map<String, Double> allocatedAmounts, Long totalCap)
    {
        List<Map<String, Double>> result = new ArrayList<>();
        /**
         * Create 2 maps to hold the split values
         */
        Map<String, Double> m1 = new HashMap<>();
        Map<String, Double> m2 = new HashMap<>();

        /**
         * Find how much we need to deduct
         */
        long total = 0;
        for (double v : allocatedAmounts.values())
        {
            total += v;
        }

        long toBeDeducted = total - totalCap;
        if (toBeDeducted <= 0)
        {
            /**
             * Return original and the zero
             */
            result.add(allocatedAmounts);
            result.add(m2);
        }
        else
        {
            long sumDeducted = 0;
            for (Map.Entry<String, Double> entry : allocatedAmounts.entrySet())
            {
                Double v = entry.getValue();
                double toBeDeductedFromThisKey = 0;
                toBeDeductedFromThisKey = Math.min((double) (toBeDeducted - sumDeducted), v);
                if (toBeDeductedFromThisKey <= 0)
                {
                    toBeDeductedFromThisKey = 0;
                }
                m1.put(entry.getKey(), v - toBeDeductedFromThisKey);
                m2.put(entry.getKey(), toBeDeductedFromThisKey);
                sumDeducted += toBeDeductedFromThisKey;
            }
            result.add(m1);
            result.add(m2);
        }

        return result;
    }

    public BookingBucketAllocation(InventoryEstimateService inventoryEstimateService, BookingBucket bookingBucket, Map<String, Booking> bookingsMapForDay,
                                   Booking booking, TargetingChannel targetingChannel, Day day, UtilVariables utilVariables)
    {
        this.inventoryEstimateService = inventoryEstimateService;
        this.bookingDao = this.inventoryEstimateService.bookingDao;
        this.tbrDao = this.inventoryEstimateService.tbrDao;
        this.inventoryEstimateDao = this.inventoryEstimateService.inventoryEstimateDao;
        this.bookingBucket = bookingBucket;
        this.bookingsMapForDay = bookingsMapForDay;
        this.booking = booking;
        this.targetingChannel = targetingChannel;
        this.day = day;
        this.utilVariables = utilVariables;
    }

    @Override
    public Object call() throws Exception
    {
        Impression inside = inventoryEstimateService.getInventoryForBookingBucketCrossQuery(bookingBucket, targetingChannel, day, bookingsMapForDay);
        long insideAmount = inside.getTotal();

        /**
         * If inside amount is zero then we cannot book.
         * atomicRequestAmount.get() == 0 means that other threads finish booking the whole requested amount.
         */
        if (insideAmount <= 0 || utilVariables.atomicRequestAmount.get() <= 0)
        {
            //System.out.println("booking bucket allocation "+insideAmount+" , "+utilVariables.atomicRequestAmount.get());
            return 0;
        }

        Impression outside = inventoryEstimateService.getInventoryForBookingBucketMinusQuery(bookingBucket, targetingChannel, day, bookingsMapForDay);
        long potentialBooking = 0;
        long amountBookedForThisBB = 0;
        /**
         * If outside is 0 then there will be no splitting.
         */
        if (outside.getTotal() == 0)
        {
            potentialBooking = insideAmount - (long) bookingBucket.getSumOfAllocatedAmounts();
            amountBookedForThisBB = calculateBookingAmount(potentialBooking, utilVariables.atomicRequestAmount, utilVariables.atomicBookedAmount);
            Map<String, Double> allocatedAmount = bookingBucket.getAllocatedAmounts();
            allocatedAmount.put(booking.getBookingId(), Double.valueOf(amountBookedForThisBB));
            bookingDao.updateBookingBucketWithAllocatedAmount(bookingBucket.getId(), allocatedAmount);
        }
        else
        {
            long used = Math.round(bookingBucket.getSumOfAllocatedAmounts()) - outside.getTotal();
            /**
             * If amount booked on the BB is less than outside means that we can use the BB after splitting.
             */
            if (used <= 0)
            {

                /**
                 * Split the BB and book on the new one
                 * 1. Current one to be removed
                 * 2. Create 2 new ones
                 */
                utilVariables.removed.add(bookingBucket);

                /**
                 * Creating booking bucket to hold existing allocation.
                 */
                BookingBucket bb1 = BookingBucket.clone(bookingBucket);
                bb1.getMinusBookingsIds().add(booking.getBookingId());
                calculateAvgTBRMap(day, bb1, bookingsMapForDay);
                utilVariables.added.add(bb1);

                /**
                 * Creating booking bucket to hold new allocation.
                 */
                BookingBucket bb2 = BookingBucket.clone(bookingBucket);
                bb2.getAndBookingsIds().add(booking.getBookingId());
                calculateAvgTBRMap(day, bb2, bookingsMapForDay);
                inside = inventoryEstimateService.getInventoryForBookingBucketCrossQuery(bb2, targetingChannel, day, bookingsMapForDay);
                potentialBooking = inside.getTotal();
                amountBookedForThisBB = calculateBookingAmount(potentialBooking, utilVariables.atomicRequestAmount, utilVariables.atomicBookedAmount);
                bb2.getAllocatedAmounts().clear();
                bb2.getAllocatedAmounts().put(booking.getBookingId(), Double.valueOf(amountBookedForThisBB));
                utilVariables.added.add(bb2);
            }
            else
            {
                /**
                 * This means the outside BB is full and the inside is partially taken.
                 */

                /**
                 * Split the BB and book on the new one
                 * 1. Current one to be removed
                 * 2. Create 2 new ones
                 */
                utilVariables.removed.add(bookingBucket);

                /**
                 * Creating booking bucket to hold existing allocation.
                 * This bb holds only outside total, so we need to adjust the
                 * holding to cap the outside value; the rest goes into inside.
                 */
                List<Map<String, Double>> bookedAmountSplitOutsideInside = splitBookedAmount(bookingBucket.getAllocatedAmounts(), outside.getTotal());

                BookingBucket bb1 = BookingBucket.clone(bookingBucket);
                bb1.getMinusBookingsIds().add(booking.getBookingId());
                calculateAvgTBRMap(day, bb1, bookingsMapForDay);
                bb1.getAllocatedAmounts().clear();
                bb1.getAllocatedAmounts().putAll(bookedAmountSplitOutsideInside.get(0));
                utilVariables.added.add(bb1);

                /**
                 * Creating booking bucket to hold new allocation plus the residual.
                 */
                BookingBucket bb2 = BookingBucket.clone(bookingBucket);
                bb2.getAndBookingsIds().add(booking.getBookingId());
                calculateAvgTBRMap(day, bb2, bookingsMapForDay);
                inside = inventoryEstimateService.getInventoryForBookingBucketCrossQuery(bb2, targetingChannel, day, bookingsMapForDay);
                potentialBooking = inside.getTotal() - used;
                amountBookedForThisBB = calculateBookingAmount(potentialBooking, utilVariables.atomicRequestAmount, utilVariables.atomicBookedAmount);
                bb2.getAllocatedAmounts().clear();
                bb2.getAllocatedAmounts().putAll(bookedAmountSplitOutsideInside.get(1));
                bb2.getAllocatedAmounts().put(booking.getBookingId(), Double.valueOf(amountBookedForThisBB));
                utilVariables.added.add(bb2);
            }
        }
        return 0;
    }

}
