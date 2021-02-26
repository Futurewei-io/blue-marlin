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

package org.apache.bluemarlin.ims.imsservice.service;

import org.apache.bluemarlin.ims.imsservice.exceptions.BookingNotFoundException;
import org.apache.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import org.apache.bluemarlin.ims.imsservice.exceptions.FailedLockingException;
import org.apache.bluemarlin.ims.imsservice.exceptions.NoInventoryExceptions;
import org.apache.bluemarlin.ims.imsservice.dao.booking.BookingDaoESImp;
import org.apache.bluemarlin.ims.imsservice.dao.inventory.InventoryEstimateDaoESImp;
import org.apache.bluemarlin.ims.imsservice.dao.tbr.TBRDao;
import org.apache.bluemarlin.ims.imsservice.model.BookResult;
import org.apache.bluemarlin.ims.imsservice.model.Booking;
import org.apache.bluemarlin.ims.imsservice.model.BookingBucket;
import org.apache.bluemarlin.ims.imsservice.model.Day;
import org.apache.bluemarlin.ims.imsservice.model.InventoryResult;
import org.apache.bluemarlin.ims.imsservice.model.Range;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class BookingService extends BaseService
{

    public static final long HIGH_PRICE = Long.MAX_VALUE;
    private static final int MAX_LOCKING_ATTEMPTS = 60;
    private static ExecutorService executor = Executors.newCachedThreadPool();

    @Autowired
    private InventoryEstimateService inventoryEstimateService;

    public BookingService(){
    }

    /**
     * This method allocate the requested inventory among the booking-buckets through following steps:
     * <p>
     * 1. Sort booking buckets
     * 2. Ignore irrelevant booking-buckets (the ones with zero cross intersection with targeting channel).
     * 3. Calculate the inventory that is outside of booking-bucket, if the amount is less than the amount booked on
     * BB then BB is full; otherwise split the BB and allocate the amount on new one.
     *
     * @param bookingDayMap
     * @param booking
     * @param targetingChannel
     * @param day
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws ESConnectionException
     */
    private void bookingAllocation(Map<Day, List<Booking>> bookingDayMap,
                                   Booking booking, TargetingChannel targetingChannel, Day day, AtomicLong atomicRequestAmount, AtomicLong atomicBookedAmount) throws IOException, JSONException, ESConnectionException, ExecutionException, InterruptedException
    {
        List<BookingBucket> removed = Collections.synchronizedList(new ArrayList<>());
        List<BookingBucket> added = Collections.synchronizedList(new ArrayList<>());

        Set<Day> daySet = new HashSet<>(Arrays.asList(new Day[]{day}));
        Map<Day, List<BookingBucket>> bookingBucketMap = this.bookingDao.getBookingBuckets(daySet);
        Map<String, Booking> bookingsMapForDay = Booking.buildBookingMap(bookingDayMap.get(day));

        if (bookingBucketMap.size() > 0 && bookingDayMap.size() > 0)
        {
            List<BookingBucket> bookingBuckets = bookingBucketMap.get(day);
            List<Future<Long>> bookedOnBookingBucketFutures = new ArrayList<>();
            for (BookingBucket bookingBucket : bookingBuckets)
            {
                if (!inventoryEstimateService.hasIntersection(bookingBucket, targetingChannel, bookingsMapForDay))
                {
                    continue;
                }

                BookingBucketAllocation bookingAllocationOnBucketTask = new BookingBucketAllocation(inventoryEstimateService, bookingBucket, bookingsMapForDay, booking, targetingChannel
                        , day, new BookingBucketAllocation.UtilVariables(added, removed, atomicRequestAmount, atomicBookedAmount));
                Future futureForBookingBucket = executor.submit(bookingAllocationOnBucketTask);
                bookedOnBookingBucketFutures.add(futureForBookingBucket);
            }

            for (Future<Long> _future : bookedOnBookingBucketFutures)
            {
                _future.get();
            }

        }

        /**
         * Deleting removed BBs and Add new ones.
         */
        for (BookingBucket bb : removed)
        {
            bookingDao.removeBookingBucket(bb);
        }

        for (BookingBucket bb : added)
        {
            bookingDao.createBookingBucket(bb);
        }

        return;
    }

    private void acquireLock() throws InterruptedException, FailedLockingException
    {
        int attempts = MAX_LOCKING_ATTEMPTS;
        while (!bookingDao.lockBooking())
        {
            Thread.sleep(1000);
            attempts--;
            if (attempts < 0)
            {
                throw new FailedLockingException();
            }
        }
    }

    public BookingService(InventoryEstimateService inventoryEstimateService, InventoryEstimateDaoESImp inventoryEstimateDao, TBRDao tbrDao, BookingDaoESImp bookingDao)
    {
        this.inventoryEstimateService = inventoryEstimateService;
        this.inventoryEstimateDao = inventoryEstimateDao;
        this.tbrDao = tbrDao;
        this.bookingDao = bookingDao;
    }

    /**
     * This service removes a booking.
     * The underlying methods find all the booking buckets that are corresponded to bookingId and set their allocation amount to zero.
     *
     * @param bookingId
     * @return
     */
    public void removeBooking(String bookingId) throws IOException, JSONException, BookingNotFoundException
    {
        bookingDao.removeBooking(bookingId);
    }

    /**
     * This service retrieves all the bookings associated to an advertiser id.
     *
     * @param advId
     * @return
     * @throws IOException
     */
    public List<Booking> getBookingsByAdvId(String advId) throws IOException, ESConnectionException, BookingNotFoundException
    {
        List<Booking> result = bookingDao.getBookings(advId);
        if (CommonUtil.isEmpty(result))
        {
            throw new BookingNotFoundException();
        }
        return result;
    }

    /**
     * This service allocate inventory for a specific targeting channel for a specific period of time.
     * The service has the following steps:
     * <p>
     * 1. Look on booking index so that no booking occur concurrently
     * 2. Check if enough inventory exists
     * 3. Create booking document in booking index
     * 4. For each day, create a booking bucket and perform booking allocation
     * 5. Unlock booking index
     *
     * @param targetingChannel
     * @param ranges
     * @param price
     * @param advId
     * @param requestCount
     * @return
     * @throws ESConnectionException
     * @throws JSONException
     * @throws IOException
     * @throws NoInventoryExceptions
     * @throws InterruptedException
     * @throws FailedLockingException
     * @throws ExecutionException
     */
    public BookResult book(TargetingChannel targetingChannel, List<Range> ranges, double price, String advId, long requestCount) throws ESConnectionException, JSONException, IOException, NoInventoryExceptions, InterruptedException, FailedLockingException, ExecutionException
    {
        /**
         * Locking booking index before start the operation.
         */
        acquireLock();

        BookResult bookResult = null;
        try
        {
            InventoryResult inventoryResult = inventoryEstimateService.aggregateInventory(targetingChannel, ranges, price);
            if (inventoryResult.getAvailCount() < requestCount)
            {
                throw new NoInventoryExceptions();
            }

            /**
             * Creating a new booking document.
             */
            Booking booking = new Booking(targetingChannel, ranges, price, advId, requestCount);
            bookingDao.createBooking(booking);

            /**
             * Creating a booking bucket for each day, if it is necessary.
             */
            List<Day> sortedDays = Day.buildSortedDays(ranges);
            Set<Day> daySet = new HashSet<>(sortedDays);
            Map<Day, List<Booking>> bookingDayMap = this.bookingDao.getBookings(daySet);
            AtomicLong atomicRequestCount = new AtomicLong(requestCount);
            AtomicLong atomicBookedAmount = new AtomicLong(0);
            List<Future<Long>> futureList = new ArrayList<>();
            for (Day day : sortedDays)
            {
                Future bookedForDayFuture = executor.submit((Callable<Object>) () ->
                {
                    BookingBucket maxPriorityBB = bookingDao.getMaxBookingBucketPriority(day.toString());
                    BookingBucket bookingBucket;

                    List<Booking> bookings = bookingDayMap.get(day);
                    List<String> previousBookingIds = new ArrayList<>();
                    int priority = 0;

                    if (maxPriorityBB != null)
                    {
                        previousBookingIds = Booking.extractBookingIds(bookings);
                        previousBookingIds.remove(booking.getBookingId());
                        priority = maxPriorityBB.getPriority() + 1;
                    }

                    bookingBucket = new BookingBucket(day.toString(), booking.getBookingId(), previousBookingIds, priority + 1);
                    bookingDao.createBookingBucket(bookingBucket);

                    /**
                     * Starting allocation process
                     */
                    bookingAllocation(bookingDayMap, booking, targetingChannel, day, atomicRequestCount, atomicBookedAmount);

                    return 0;
                });
                futureList.add(bookedForDayFuture);
            }

            for (Future _future : futureList)
            {
                _future.get();
            }

            bookResult = new BookResult(booking.getBookingId(), atomicBookedAmount.get());
        }
        catch (Exception e)
        {
            throw e;
        }

        finally
        {
            bookingDao.unlockBooking();
        }

        return bookResult;
    }

    public Booking getBookingById(String bookingId) throws IOException, ESConnectionException
    {
        return bookingDao.getBooking(bookingId);
    }
}
