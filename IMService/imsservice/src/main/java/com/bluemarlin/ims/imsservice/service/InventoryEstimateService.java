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

import com.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import com.bluemarlin.ims.imsservice.dao.booking.BookingDaoESImp;
import com.bluemarlin.ims.imsservice.dao.inventory.InventoryEstimateDaoESImp;
import com.bluemarlin.ims.imsservice.dao.tbr.TBRDao;
import com.bluemarlin.ims.imsservice.model.Booking;
import com.bluemarlin.ims.imsservice.model.BookingBucket;
import com.bluemarlin.ims.imsservice.model.Day;
import com.bluemarlin.ims.imsservice.model.DayImpression;
import com.bluemarlin.ims.imsservice.model.Impression;
import com.bluemarlin.ims.imsservice.model.InventoryResult;
import com.bluemarlin.ims.imsservice.model.Range;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import com.bluemarlin.ims.imsservice.util.TargetingChannelUtil;
import javafx.util.Pair;
import org.json.JSONException;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
public class InventoryEstimateService extends BaseService
{

    private static final int MAX_TIME_SIZE = 100;
    private static ExecutorService executor = Executors.newCachedThreadPool();

    public InventoryEstimateService()
    {
    }

    /**
     * This method filters out the booking buckets that do not have intersection with targeting channel.
     *
     * @param targetingChannel
     * @param bookingBuckets
     * @param bookingsMapForDay
     * @return
     */
    private List<BookingBucket> filterBookingBuckets(TargetingChannel targetingChannel, List<BookingBucket> bookingBuckets, Map<String, Booking> bookingsMapForDay)
    {
        List<BookingBucket> filteredBookingBuckets = new ArrayList<>();
        if (!CommonUtil.isEmpty(bookingBuckets))
        {
            for (BookingBucket bookingBucket : bookingBuckets)
            {
                if (bookingBucket.getSumOfAllocatedAmounts() != 0 ||
                        hasIntersection(bookingBucket, targetingChannel, bookingsMapForDay))
                {
                    filteredBookingBuckets.add(bookingBucket);
                }
            }
        }
        return filteredBookingBuckets;
    }

    /**
     * This method returns AVAILABLE inventory for a specific targeting channel in a period of time through the following steps:
     * <p>
     * 1. Get the total inventory for the targeting channel in period of time; this value does not consider bookings.
     * 2. Calculate how much of the inventory is taken by each booking-bucket for each day.
     * 3. Subtract the total amount booked from total inventory.
     * <p>
     * This method in multi-thread function.
     *
     * @param targetingChannel
     * @param ranges
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    private Pair<Impression, Long> aggregateInventory(TargetingChannel targetingChannel, List<Range> ranges) throws JSONException, ESConnectionException, IOException, ExecutionException, InterruptedException
    {
        Impression result = new Impression();

        if (ranges.size() == 0 || ranges.size() > MAX_TIME_SIZE)
        {
            LOGGER.error("ranges size", ranges.size());
            return new Pair(result, 0L);
        }

        List<Day> sortedDays = Day.buildSortedDays(ranges);

        Map<Day, Impression> potentialInventory = this.getPotentialInventoryForDays(targetingChannel, new HashSet<>(sortedDays));
        Map<Day, List<BookingBucket>> bookingBucketMap = this.bookingDao.getBookingBuckets(new HashSet<>(sortedDays));
        Map<Day, List<Booking>> bookings = this.bookingDao.getBookings(new HashSet<>(sortedDays));

        Map<Day, Future<Long>> futureMap = new HashMap<>();
        for (Day day : sortedDays)
        {
            /**
             * It holds the amount that is booked for a day.
             */
            Future bookedForDayFuture = executor.submit((Callable<Object>) () ->
            {
                Map<String, Booking> bookingsMapForDay = Booking.buildBookingMap(bookings.get(day));
                List<BookingBucket> bookingBuckets = bookingBucketMap.get(day);

                /**
                 * Filtering the bookingBuckets.
                 */
                List<BookingBucket> filteredBookingBuckets = filterBookingBuckets(targetingChannel, bookingBuckets, bookingsMapForDay);

                long consideredBookedForDay = 0;
                List<Future<Long>> bookedOnBookingBucketFutures = new ArrayList<>();
                for (BookingBucket bookingBucket : filteredBookingBuckets)
                {
                    Future futureForBookingBucket = executor.submit((Callable<Object>) () ->
                    {
                        long consideredBookedFor1Day1BB = 0;
                        Impression outside = getInventoryForBookingBucketMinusQuery(bookingBucket, targetingChannel, day, bookingsMapForDay);
                        long outsideOverflow = Math.round(bookingBucket.getSumOfAllocatedAmounts()) - outside.getTotal();

                        /**
                         * If outside is less than allocated then outside-overflow is considered booked.
                         * In other words consider outsideOverflow if it is more than 0.
                         */
                        consideredBookedFor1Day1BB += Math.max(outsideOverflow, 0);

                        return consideredBookedFor1Day1BB;
                    });
                    bookedOnBookingBucketFutures.add(futureForBookingBucket);
                }

                for (Future<Long> _future : bookedOnBookingBucketFutures)
                {
                    long count = _future.get();
                    consideredBookedForDay += count;
                }

                return consideredBookedForDay;
            });

            futureMap.put(day, bookedForDayFuture);
        }

        long consideredBooked = 0;
        for (Day day : sortedDays)
        {
            long count = futureMap.get(day).get();
            consideredBooked += count;
        }

        Impression totalInventory = new Impression();
        for (Impression impression : potentialInventory.values())
        {
            totalInventory = Impression.add(totalInventory, impression);
        }

        return new Pair(totalInventory, consideredBooked);
    }


    /**
     * This method returns total inventory (ignoring bookings) for a collection of targeting channels.
     *
     * @param tcs
     * @param day
     * @return
     * @throws JSONException
     * @throws IOException
     * @throws ESConnectionException
     */
    private Impression getInventoryForDay_PI_TCs(List<TargetingChannel> tcs,
                                                 Day day) throws JSONException, IOException, ESConnectionException
    {
        Impression impression = inventoryEstimateDao.getPredictions_PI_TCs(day, tcs);
        double tbr = tbrDao.getTBRRatio_PI_TCs(tcs);
        return Impression.multiply(impression, tbr);
    }

    /**
     * This method return total inventory ignoring bookings.
     * It considers TBR and region distribution.
     *
     * @param targetingChannel
     * @param days
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     * @throws IOException
     */
    private Map<Day, Impression> getPotentialInventoryForDays(TargetingChannel targetingChannel, Set<Day> days) throws JSONException, ESConnectionException, IOException
    {
        Map<Day, Impression> predictions = this.inventoryEstimateDao.aggregatePredictionsFullDays(targetingChannel, days);
        Map<Day, Impression> result = new HashMap<>();
        if ((targetingChannel.hasMultiValues()))
        {
            LOGGER.debug("targeting channel has multi values");
            double ratio = tbrDao.getTBRRatio(targetingChannel);
            for (Map.Entry<Day, Impression> entry : predictions.entrySet())
            {
                Day day = entry.getKey();
                Impression impression = entry.getValue();
                impression = Impression.multiply(impression, ratio);
                result.put(day, impression);
            }
        }
        else
        {
            result = predictions;
        }

        return result;
    }

    public InventoryEstimateService(InventoryEstimateDaoESImp inventoryEstimateDao, TBRDao tbrDao, BookingDaoESImp bookingDao)
    {
        this.inventoryEstimateDao = inventoryEstimateDao;
        this.tbrDao = tbrDao;
        this.bookingDao = bookingDao;
        LOGGER.debug("process InventoryEstimateService");
    }

    /**
     * This service returns available inventory for a specific targeting channel in a period of time.
     * <p>
     * THIS SERVICE DOES NOT CONSIDER REGION RATIOS. Refer to ERD for details.
     *
     * @param targetingChannel
     * @param ranges
     * @param price
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     * @throws IOException
     */
    public InventoryResult aggregateInventory(TargetingChannel targetingChannel, List<Range> ranges, double price) throws JSONException, ESConnectionException, IOException, ExecutionException, InterruptedException
    {
        InventoryResult result = new InventoryResult();
        Pair<Impression, Long> inventoryValue = aggregateInventory(targetingChannel, ranges);
        long value = inventoryValue.getKey().countImpressions(price, targetingChannel.getPm());
        long booked = inventoryValue.getValue();
        value -= booked;
        if (value < 0)
        {
            LOGGER.info("Estimate impressions < 0");
            value = 0;
        }
        result.setAvailCount(value);
        LOGGER.info("inventory estimate:{}", value);
        return result;
    }

    /**
     * The method returns a list of targeting channels associated with booking ids. It reflects the order of the bookings ids.
     *
     * @return
     */
    public List<TargetingChannel> extractTargetingChannelsFromBookingsByBookingIds(Map<String, Booking> bookingsMap, List<String> bookingIds)
    {
        List<TargetingChannel> result = new ArrayList<>();
        for (String bookingId : bookingIds)
        {
            if (bookingsMap.containsKey(bookingId))
            {
                Booking booking = bookingsMap.get(bookingId);
                result.add(booking.getQuery());
            }
        }
        return result;
    }

    /**
     * This method returns true if booking-bucket and targeting channel have intersection.
     *
     * @param bookingBucket
     * @param targetingChannel
     * @param bookingsMap
     * @return
     */
    public boolean hasIntersection(BookingBucket bookingBucket, TargetingChannel targetingChannel, Map<String, Booking> bookingsMap)
    {
        List<TargetingChannel> bns = extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, bookingBucket.getAndBookingsIds());
        bns.add(targetingChannel);
        return TargetingChannelUtil.hasIntersectionsForSingleAttributes(bns);
    }

    /**
     * This method returns total inventory ignoring bookings for the following complex targeting channel collections :
     * <p>
     * (Bn1 and Bn2 and ...) - (Bm1 or Bm2 or ... or query)
     * <p>
     * Each Bn and Bm is a targeting channel.
     *
     * @param day
     * @param bns
     * @param bms
     * @param query
     * @param avgTbr        :  This is avg TBR of Bms
     * @param maxImpression : This is the maximum value of [(PBm1+PBm2+Pq)&(PBn)].avrTbr
     * @return
     * @throws ESConnectionException
     * @throws IOException
     * @throws JSONException
     */
    public Impression getInventoryFor_BNs_Minus_BMs_Minus_q(Day day, List<TargetingChannel> bns, List<TargetingChannel> bms, TargetingChannel query, double avgTbr, Impression maxImpression) throws ESConnectionException, IOException, JSONException
    {
        /**
         * result = (PBns.TBns - [(PBm1+PBm2+Pq)&(PBn)].avrTbr
         */
        Impression impression1 = getInventoryForDay_PI_TCs(bns, day);

        List<TargetingChannel> Bms_plus_q = new ArrayList<>(bms);
        Bms_plus_q.add(query);
        Impression PBms_plus_Pq_and_Bns = inventoryEstimateDao.getPredictions_SIGMA_TCs_PI_BNs(day, Bms_plus_q, bns);
        Impression impression2 = Impression.multiply(PBms_plus_Pq_and_Bns, avgTbr);

        /**
         * [(PBm1+PBm2+Pq)&(PBn)].avrTbr has the min value of
         * max of [(PBm(i))&(PBn)].Tbr(Bm(i), Bn)
         */
        if (impression2.getTotal() < maxImpression.getTotal())
        {
            impression2 = maxImpression;
        }

        Impression result = Impression.subtract(impression1, impression2);

        result.adjustPositive();

        return result;
    }

    /**
     * This method returns total inventory ignoring bookings for the following complex targeting channel collections :
     * <p>
     * (Bn1 and Bn2 and ...) and query  - (Bm1 or Bm2 or ... )
     * <p>
     * Each Bn and Bm is a targeting channel.
     *
     * @param day
     * @param bns
     * @param query
     * @param bms
     * @param avgTbr
     * @param maxImpression
     * @return
     * @throws ESConnectionException
     * @throws IOException
     * @throws JSONException
     */
    public Impression getInventoryFor_BNs_DOT_q_Minus_BMs(Day day, List<TargetingChannel> bns, TargetingChannel query, List<TargetingChannel> bms, double avgTbr, Impression maxImpression) throws ESConnectionException, IOException, JSONException
    {
        /**
         * result = (PBns.TBns - [(PBm1+PBm2+~Pq)&(PBns)].avrTbr - (Pq&(Bns-Bms))*(Tn^TqBar)
         *
         * The output of this method might be negative because of avgTbr. The result is floored at 0.
         */
        Impression impression1 = getInventoryForDay_PI_TCs(bns, day);

        Impression impression2 = inventoryEstimateDao.getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs(day, bms, query, bns);
        impression2 = Impression.multiply(impression2, avgTbr);

        /**
         * [(PBm1+PBm2+~Pq)&(PBns)].avrTbr has the min value of
         * max of [(PBm(i))&(PBn)].Tbr(Bm(i), Bn)
         */
        if (impression2.getTotal() < maxImpression.getTotal())
        {
            impression2 = maxImpression;
        }

        Impression impression3 = inventoryEstimateDao.getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q(day, bns, bms, query);
        double tbr = tbrDao.getTBRRatio_PI_BNs_DOT_qBAR(bns, query);

        impression3 = Impression.multiply(impression3, tbr);

        Impression result = Impression.subtract(impression1, impression2);
        result = Impression.subtract(result, impression3);

        result.adjustPositive();

        return result;
    }

    /**
     * This method returns total inventory (ignoring bookings) that is associated with
     * one booking-bucket minus a targeting channel.
     *
     * @param bookingBucket
     * @param query
     * @param day
     * @param bookingsMap
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws ESConnectionException
     */
    public Impression getInventoryForBookingBucketMinusQuery(BookingBucket bookingBucket, TargetingChannel query, Day day, Map<String, Booking> bookingsMap) throws IOException, JSONException, ESConnectionException
    {
        List<TargetingChannel> Bns = extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, bookingBucket.getAndBookingsIds());
        List<TargetingChannel> Bms = extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, bookingBucket.getMinusBookingsIds());
        if (CommonUtil.isEmpty(Bns))
        {
            LOGGER.info("Bookings of BB have been removed");
            return new Impression();
        }

        Pair<Double, Impression> result = getAverageTBRForBookingBucketMinusQ(day, bookingBucket, Bns, query, bookingsMap.keySet());
        double avgTbr = result.getKey();
        Impression maxImpression = result.getValue();
        Impression impression = getInventoryFor_BNs_Minus_BMs_Minus_q(day, Bns, Bms, query, avgTbr, maxImpression);
        return impression;
    }

    /**
     * This method returns total inventory (ignoring bookings) that is associated with intersection of
     * one booking-bucket and a targeting channel.
     *
     * @param bookingBucket
     * @param query
     * @param day
     * @param bookingsMap
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws ESConnectionException
     */
    public Impression getInventoryForBookingBucketCrossQuery(BookingBucket bookingBucket, TargetingChannel query, Day day, Map<String, Booking> bookingsMap) throws IOException, JSONException, ESConnectionException
    {
        List<TargetingChannel> Bns = extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, bookingBucket.getAndBookingsIds());
        List<TargetingChannel> Bms = extractTargetingChannelsFromBookingsByBookingIds(bookingsMap, bookingBucket.getMinusBookingsIds());
        if (CommonUtil.isEmpty(Bns))
        {
            LOGGER.info("Bookings of BB have been removed");
            return new Impression();
        }

        Pair<Double, Impression> result = getAverageTBRForBookingBucketMinusQBar(day, bookingBucket, Bns, query, bookingsMap.keySet());
        double avgTbr = result.getKey();
        Impression maxImpression = result.getValue();
        Impression impression = getInventoryFor_BNs_DOT_q_Minus_BMs(day, Bns, query, Bms, avgTbr, maxImpression);
        return impression;
    }

    /**
     * This method returns average TBR of a booking-bucket minus targeting channel with its possible maximum value (max of ims/inv).
     *
     * @param day
     * @param bb
     * @param Bns
     * @param query
     * @param validBookingIds
     * @return
     * @throws IOException
     * @throws JSONException
     */
    public Pair<Double, Impression> getAverageTBRForBookingBucketMinusQ(Day day, BookingBucket bb, List<TargetingChannel> Bns, TargetingChannel query, Set<String> validBookingIds) throws IOException, JSONException
    {
        BookingBucket.AvrTBRInsight avrTBRInsight = bb.getAvrTBRInsight(validBookingIds);
        double nominator = avrTBRInsight.getNominator();
        double denominator = avrTBRInsight.getDenominator();
        Impression maxImpression = avrTBRInsight.getMaxImpression();

        List<TargetingChannel> _tcs = new ArrayList<>();
        _tcs.addAll(Bns);
        _tcs.add(query);

        Impression impression = inventoryEstimateDao.getPredictions_PI_TCs(day, _tcs);
        double tbr = tbrDao.getTBRRatio_PI_TCs(_tcs);
        double impressionValue = impression.getTotal();

        if (impressionValue * tbr > maxImpression.getTotal())
        {
            maxImpression = Impression.multiply(impression, tbr);
        }

        nominator += impressionValue * tbr;
        denominator += impressionValue;

        return buildTBRResponse(nominator, denominator, maxImpression);
    }

    /**
     * This method returns average TBR of a booking-bucket minus inverse of a targeting channel with its possible maximum value (max of ims/inv).
     *
     * @param day
     * @param bb
     * @param Bns
     * @param query
     * @param validBookingIds
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws ESConnectionException
     */
    public Pair<Double, Impression> getAverageTBRForBookingBucketMinusQBar(Day day, BookingBucket bb, List<TargetingChannel> Bns, TargetingChannel query, Set<String> validBookingIds) throws IOException, JSONException, ESConnectionException
    {
        BookingBucket.AvrTBRInsight avrTBRInsight = bb.getAvrTBRInsight(validBookingIds);
        double nominator = avrTBRInsight.getNominator();
        double denominator = avrTBRInsight.getDenominator();
        Impression maxImpression = avrTBRInsight.getMaxImpression();

        Impression impression = inventoryEstimateDao.getPredictions_PI_TCs_DOT_qBAR(day, Bns, query);

        /**
         * Q bar does not have tbr part
         */
        double tbr = tbrDao.getTBRRatio_PI_TCs(Bns);
        double impressionValue = impression.getTotal();

        if (impressionValue * tbr > maxImpression.getTotal())
        {
            maxImpression = Impression.multiply(impression, tbr);
        }

        nominator += impressionValue * tbr;
        denominator += impressionValue;

        return buildTBRResponse(nominator, denominator, maxImpression);
    }

    /**
     * This service returns the last predicted total hourly impressions for a targeting channel ignoring bookings.
     * This method is used for chart and publishing purposes.
     *
     * @param targetingChannel
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     */
    public DayImpression getInventoryDateEstimate(
            TargetingChannel targetingChannel) throws JSONException, ESConnectionException, IOException
    {

        double tbrRatio = 1;
        if (!(targetingChannel.hasMultiValues()))
        {
            LOGGER.debug("targeting channel has no multi values");
        }
        else
        {
            LOGGER.debug("targeting channel has multi values");
            tbrRatio = tbrDao.getTBRRatio(targetingChannel);
            LOGGER.info("tbrRatio=", tbrRatio);
        }

        return inventoryEstimateDao.getHourlyPredictions(targetingChannel, tbrRatio);
    }

    private Pair<Double, Impression> buildTBRResponse(double nominator, double denominator, Impression maxImpression)
    {
        if (CommonUtil.equalNumbers(nominator, denominator))
        {
            return new Pair(1.0, maxImpression);
        }

        if (nominator == 0)
        {
            return new Pair(0.0, maxImpression);
        }

        return new Pair(nominator / denominator, maxImpression);
    }

}

