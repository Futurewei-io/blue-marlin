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

package org.apache.bluemarlin.ims.imsservice.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class Booking
{
    @JsonProperty("days")
    private List<String> days;

    @JsonProperty("bk_id")
    private String bookingId;

    @JsonProperty("adv_id")
    private String advId;

    @JsonProperty("price")
    private Double price;

    @JsonProperty("amount")
    private Long amount;

    @JsonProperty("query")
    private TargetingChannel query;

    @JsonProperty("del")
    private boolean deleted;

    @JsonIgnore
    private String id;


    public static Number extractNumber(Object obj)
    {
        if (obj == null)
        {
            return null;
        }
        try
        {
            return Double.valueOf(obj.toString());
        }
        catch (Exception e)
        {
            /**
             * Safe exception
             */
        }
        return null;
    }

    public static Map<String, Booking> buildBookingMap(List<Booking> bookings)
    {
        Map<String, Booking> bookingMap = new HashMap<>();
        if (bookings != null)
        {
            for (Booking booking : bookings)
            {
                bookingMap.put(booking.getBookingId(), booking);
            }
        }
        return bookingMap;
    }

    public static List<String> extractBookingIds(List<Booking> bookings)
    {
        List<String> result = new ArrayList<>();
        for (Booking booking : bookings)
        {
            result.add(booking.getBookingId());
        }
        return result;
    }

    public Booking(TargetingChannel targetingChannel, List<Range> ranges, double price, String advId, long requestCount)
    {
        List<Day> dayList = Day.buildSortedDays(ranges);
        Set<String> dayStrSet = Day.getDayStringTreated(new HashSet<>(dayList), "-");
        List<String> dayStrList = new ArrayList<>(dayStrSet);
        Collections.sort(dayStrList);
        this.days = dayStrList;
        this.price = price;
        this.amount = requestCount;
        this.advId = advId;
        this.query = targetingChannel;
        this.bookingId = UUID.randomUUID().toString();
    }

    public Booking(String id, Map source)
    {
        this.id = id;
        this.days = (List<String>) source.get("days");
        this.bookingId = String.valueOf(source.get("bk_id"));
        Number tmpNumber = extractNumber(source.get("price"));
        this.price = (tmpNumber != null) ? tmpNumber.doubleValue() : null;
        tmpNumber = extractNumber(source.get("amount"));
        this.amount = (tmpNumber != null) ? tmpNumber.longValue() : null;
        this.advId = String.valueOf(source.get("adv_id"));
        this.query = TargetingChannel.build((Map) source.get("query"));
        this.deleted = (boolean) source.get("del");
    }

    public String getId()
    {
        return id;
    }

    public List<String> getDays()
    {
        return days;
    }

    public String getBookingId()
    {
        return bookingId;
    }

    public String getAdvId()
    {
        return advId;
    }

    public Double getPrice()
    {
        return price;
    }

    public Long getAmount()
    {
        return amount;
    }

    public TargetingChannel getQuery()
    {
        return query;
    }
}
