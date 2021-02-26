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

package org.apache.bluemarlin.ims.imsservice.dao.booking;

import org.apache.bluemarlin.ims.imsservice.exceptions.BookingNotFoundException;
import org.apache.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import org.apache.bluemarlin.ims.imsservice.model.Booking;
import org.apache.bluemarlin.ims.imsservice.model.BookingBucket;
import org.apache.bluemarlin.ims.imsservice.model.Day;
import org.apache.bluemarlin.ims.imsservice.dao.BaseDao;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface identifies the methods that are required by services.
 * Any database that keeps booking related data needs to implement this interface.
 */
public interface BookingDao extends BaseDao
{
    /**
     * This method removes the booking document and set the booked amount on the related booking-bucket to zero.
     * @param bookingId
     * @throws IOException
     * @throws JSONException
     * @throws BookingNotFoundException
     */
    void removeBooking(String bookingId) throws IOException, BookingNotFoundException;

    /**
     * This method adds a booking document to elasticsearch.
     * @param booking
     * @throws IOException
     */
    String createBooking(Booking booking) throws ESConnectionException, IOException;

    /**
     * This method returns bookings in an organized form, (day,bookings) map.
     * This is used to find all the bookings for a day.
     * @param days
     * @return
     * @throws JSONException
     * @throws IOException
     */
    Map<Day, List<Booking>> getBookings(Set<Day> days) throws ESConnectionException, IOException;

    /**
     * This method returns list of bookings for an advertiser id.
     * @param advId
     * @return
     * @throws JSONException
     * @throws IOException
     */
    List<Booking> getBookings(String advId) throws JSONException, ESConnectionException, IOException;

    /**
     * This method returns a booking.
     * @param bookingId
     * @return
     * @throws JSONException
     * @throws IOException
     */
    Booking getBooking(String bookingId) throws JSONException, ESConnectionException, IOException;

    /**
     * This method returns booking-buckets for a set of days in an organized form, (day, booking-buckets) map.
     * @param days
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Map<Day, List<BookingBucket>> getBookingBuckets(Set<Day> days) throws IOException;

    /**
     * This method adds a new booking-bucket to elasticsearch.
     * @param bookingBucket
     * @throws IOException
     */
    String createBookingBucket(BookingBucket bookingBucket) throws IOException;

    /**
     * This method return True if it successfully adds a new document to the booking index,
     * If the document already exists, the method returns False.
     * This method is used to globally lock booking index.
     * @return
     */
    boolean lockBooking();

    /**
     * This method deletes LOCK_BOOKING_ID document from booking index.
     * @throws IOException
     */
    void unlockBooking() throws IOException;

    /**
     * This method returns a booking bucket with maximum priority.
     * If more that one document with maximum priority exists, the method returns the first hit.
     * @param day
     * @return
     * @throws IOException
     * @throws JSONException
     */
    BookingBucket getMaxBookingBucketPriority(String day) throws IOException;

    /**
     * This method update booking-bucket document with a new allocated-amount.
     * @param bookingBucketId
     * @param newAllocatedAmount
     * @throws IOException
     */
    void updateBookingBucketWithAllocatedAmount(String bookingBucketId, Map<String, Double> newAllocatedAmount) throws IOException;

    /**
     * This method returns all the booking-buckets that are related to one booking id.
     * @param bookingId
     * @return
     * @throws IOException
     * @throws JSONException
     */
    List<BookingBucket> findBookingBucketsWithAllocatedBookingId(String bookingId) throws IOException;

    /**
     * This method delete a booking-bucket from elasticsearch.
     * @param bookingBucket
     * @throws IOException
     */
    void removeBookingBucket(BookingBucket bookingBucket) throws IOException;
}
