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

package com.bluemarlin.ims.imsservice.dao.booking;

import com.bluemarlin.ims.imsservice.esclient.ESResponse;
import com.bluemarlin.ims.imsservice.exceptions.BookingNotFoundException;
import com.bluemarlin.ims.imsservice.model.Booking;
import com.bluemarlin.ims.imsservice.model.BookingBucket;
import com.bluemarlin.ims.imsservice.model.Day;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import com.bluemarlin.ims.imsservice.util.IMSLogger;
import com.bluemarlin.ims.imsservice.dao.BaseDaoESImp;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Repository
public class BookingDaoESImp extends BaseDaoESImp implements BookingDao
{
    private static final IMSLogger LOGGER = IMSLogger.instance();
    private static final int MAX_BOOKING_READ_PER_DAY = 1000;
    private static final String LOCK_BOOKING_ID = "lockBooking";
    protected static final String SOURCE = "_source";

    public BookingDaoESImp(){
    }

    public BookingDaoESImp(Properties properties)
    {
        super(properties);
    }

    private void removeBookingDoc(String bookingId) throws IOException, BookingNotFoundException
    {
        Booking booking = getBooking(bookingId);
        if (booking == null)
        {
            throw new BookingNotFoundException();
        }
        UpdateRequest updateRequest = new UpdateRequest(this.bookingsIndex, ES_TYPE, booking.getId());
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("del", true);
        updateRequest.doc(requestMap);
        esclient.update(updateRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
    }

    private Booking buildBooking(Map doc)
    {
        try
        {
            Booking booking = new Booking(String.valueOf(doc.get("_id")), (Map) doc.get(SOURCE));
            return booking;
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    private BookingBucket buildBookingBucket(Map doc)
    {
        try
        {
            BookingBucket bookingBucket = new BookingBucket(String.valueOf(doc.get("_id")), (Map) doc.get(SOURCE));
            return bookingBucket;
        }
        catch (Exception e)
        {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    private Map<String, List<Booking>> extractBookings(List docs)
    {
        Map<String, List<Booking>> result = new HashMap<>();
        for (Object doc : docs)
        {
            Map docMap = (Map) doc;
            Booking booking = buildBooking(docMap);
            if (booking == null)
            {
                continue;
            }
            for (String dayStr : booking.getDays())
            {
                if (!result.containsKey(dayStr))
                {
                    result.put(dayStr, new ArrayList<>());
                }
                result.get(dayStr).add(booking);
            }
        }
        return result;
    }

    private List<Booking> extractBookingsList(List docs)
    {
        List<Booking> result = new ArrayList<>();
        for (Object doc : docs)
        {
            Map mapDoc = (Map) doc;
            Booking booking = buildBooking(mapDoc);
            if (booking == null)
            {
                continue;
            }
            result.add(booking);
        }
        return result;
    }

    @Override
    public void removeBooking(String bookingId) throws IOException, BookingNotFoundException
    {
        /**
         * remove the booking
         */
        removeBookingDoc(bookingId);

        /**
         * Update booking buckets
         */
        List<BookingBucket> bookingBuckets = findBookingBucketsWithAllocatedBookingId(bookingId);
        for (BookingBucket bookingBucket : bookingBuckets)
        {
            bookingBucket.getAllocatedAmounts().put(bookingId, 0.0);
            updateBookingBucketWithAllocatedAmount(bookingBucket.getId(), bookingBucket.getAllocatedAmounts());
        }
    }

    @Override
    public String createBooking(Booking booking) throws IOException
    {
        Map sourceMap = CommonUtil.convertToMap(booking);
        IndexRequest indexRequest = new IndexRequest(this.bookingsIndex, ES_TYPE);
        indexRequest.source(sourceMap);
        IndexResponse indexResponse = esclient.index(indexRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
        return indexResponse.getId();
    }

    @Override
    public Map<Day, List<Booking>> getBookings(Set<Day> days) throws IOException
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchRequest searchRequest;
        List<String> dayStrs = new ArrayList(Day.getDayStringTreated(days, "-"));
        append(boolQueryBuilder, dayStrs, "days");
        append(boolQueryBuilder, "del", false);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(MAX_BOOKING_READ_PER_DAY);
        searchRequest = new SearchRequest(this.bookingsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        List docs = (List) ((Map) (esResponse.getSourceMap().get("hits"))).get("hits");
        Map<String, List<Booking>> dayStringBookingMap = extractBookings(docs);

        Map<Day, List<Booking>> result = new HashMap<>();
        for (Map.Entry<String, List<Booking>> entry : dayStringBookingMap.entrySet())
        {
            result.put(new Day(entry.getKey()), entry.getValue());
        }
        return result;
    }

    @Override
    public List<Booking> getBookings(String advId) throws IOException
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchRequest searchRequest;
        append(boolQueryBuilder, "adv_id", advId);
        append(boolQueryBuilder, "del", false);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(MAX_BOOKING_READ_PER_DAY);
        searchRequest = new SearchRequest(this.bookingsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        List docs = (List) ((Map) (esResponse.getSourceMap().get("hits"))).get("hits");
        List<Booking> result = extractBookingsList(docs);

        return result;
    }

    @Override
    public Booking getBooking(String bookingId) throws IOException
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchRequest searchRequest;
        append(boolQueryBuilder, "bk_id", bookingId);
        append(boolQueryBuilder, "del", false);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(MAX_BOOKING_READ_PER_DAY);
        searchRequest = new SearchRequest(this.bookingsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        List docs = (List) ((Map) (esResponse.getSourceMap().get("hits"))).get("hits");
        for (Object doc : docs)
        {
            Map mapDoc = (Map) doc;
            Booking booking = buildBooking(mapDoc);
            if (booking != null)
            {
                return booking;
            }
        }
        return null;
    }

    @Override
    public Map<Day, List<BookingBucket>> getBookingBuckets(Set<Day> days) throws IOException
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        SearchRequest searchRequest;
        List<String> dayStrs = new ArrayList(Day.getDayStringTreated(days, "-"));
        append(boolQueryBuilder, dayStrs, "day");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(MAX_BOOKING_READ_PER_DAY);
        searchRequest = new SearchRequest(this.bookingBucketsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        Map<Day, List<BookingBucket>> result = new HashMap<>();
        if (esResponse == null)
        {
            LOGGER.error("response is null");
            return result;
        }

        List docs = (List) ((Map) (esResponse.getSourceMap().get("hits"))).get("hits");
        for (Object doc : docs)
        {
            Map docMap = (Map) doc;
            BookingBucket bookingBucket = buildBookingBucket(docMap);
            if (bookingBucket == null)
            {
                continue;
            }
            Day day = new Day(bookingBucket.getDay());
            if (!result.containsKey(day))
            {
                result.put(day, new ArrayList<BookingBucket>());
            }
            result.get(day).add(bookingBucket);
        }
        return result;
    }

    public String createBookingBucket(BookingBucket bookingBucket) throws IOException
    {
        Map sourceMap = CommonUtil.convertToMap(bookingBucket);
        IndexRequest indexRequest = new IndexRequest(this.bookingBucketsIndex, ES_TYPE);
        indexRequest.source(sourceMap);
        IndexResponse indexResponse = esclient.index(indexRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
        return indexResponse.getId();
    }

    @Override
    public boolean lockBooking()
    {
        try
        {
            IndexRequest indexRequest = new IndexRequest(this.bookingsIndex, ES_TYPE);
            indexRequest.id(LOCK_BOOKING_ID);
            indexRequest.create(true);
            indexRequest.source(new HashMap());
            esclient.index(indexRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }

    @Override
    public void unlockBooking() throws IOException
    {
        DeleteRequest deleteRequest = new DeleteRequest(this.bookingsIndex);
        deleteRequest.type(ES_TYPE);
        deleteRequest.id(LOCK_BOOKING_ID);
        this.esclient.delete(deleteRequest);
    }

    @Override
    public BookingBucket getMaxBookingBucketPriority(String day) throws IOException
    {
        /**
         * Make sure day is in right format
         */
        day = day.replace("-", "");

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        append(boolQueryBuilder, Arrays.asList(new String[]{day}), "day");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(0);

        TopHitsAggregationBuilder topHitsAggregationBuilder = new TopHitsAggregationBuilder("doc_with_max_priority");
        topHitsAggregationBuilder.size(1).sort("priority", SortOrder.DESC);
        sourceBuilder = sourceBuilder.aggregation(topHitsAggregationBuilder);

        SearchRequest searchRequest = new SearchRequest(this.bookingBucketsIndex);
        searchRequest.source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        BookingBucket result = null;
        Object obj = ((Map) (((Map) (esResponse.getSourceMap().get("top_hits#doc_with_max_priority"))).get("hits"))).get("hits");
        List<Object> bbList = (List<Object>) obj;
        if (bbList.size() > 0)
        {
            Map bbMap = (Map) bbList.get(0);
            result = buildBookingBucket(bbMap);
        }

        return result;
    }

    @Override
    public void updateBookingBucketWithAllocatedAmount(String bookingBucketId, Map<String, Double> newAllocatedAmount) throws IOException
    {
        UpdateRequest updateRequest = new UpdateRequest(this.bookingBucketsIndex, ES_TYPE, bookingBucketId);
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("allocated_amount", newAllocatedAmount);
        updateRequest.doc(requestMap);
        esclient.update(updateRequest, WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
    }

    @Override
    public List<BookingBucket> findBookingBucketsWithAllocatedBookingId(String bookingId) throws IOException
    {
        ExistsQueryBuilder existsQueryBuilder = new ExistsQueryBuilder("allocated_amount." + bookingId);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(existsQueryBuilder);
        SearchRequest searchRequest = new SearchRequest(this.bookingBucketsIndex);
        searchRequest.source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);

        List<BookingBucket> result = new ArrayList<>();
        List docs = (List) ((Map) (esResponse.getSourceMap().get("hits"))).get("hits");
        for (Object doc : docs)
        {
            Map docMap = (Map) doc;
            BookingBucket bookingBucket = buildBookingBucket(docMap);
            if (bookingBucket == null)
            {
                continue;
            }
            result.add(bookingBucket);
        }

        return result;
    }

    @Override
    public void removeBookingBucket(BookingBucket bookingBucket) throws IOException
    {
        DeleteRequest deleteRequest = new DeleteRequest(this.bookingBucketsIndex);
        deleteRequest.type(ES_TYPE);
        deleteRequest.id(bookingBucket.getId());
        this.esclient.delete(deleteRequest);
    }

}
