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

package com.bluemarlin.ims.imsservice.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BookingBucket implements Comparable
{
    public interface AvrTBRInsight
    {
        double getNominator();

        double getDenominator();

        Impression getMaxImpression();
    }

    @JsonProperty("day")
    private String day;

    @JsonProperty("and_bk_ids")
    private List<String> andBookingsIds;

    @JsonProperty("minus_bk_ids")
    private List<String> minusBookingsIds;

    @JsonProperty("allocated_amount")
    private Map<String, Double> allocatedAmounts;

    @JsonProperty("avg_tbr_map")
    private Map<String, List<Double>> avgTbrMap;

    @JsonProperty("priority")
    private Integer priority;

    @JsonIgnore
    private String id;

    public BookingBucket()
    {

    }

    public static BookingBucket clone(BookingBucket bb)
    {
        ObjectMapper oMapper = new ObjectMapper();
        Map map = oMapper.convertValue(bb, Map.class);
        BookingBucket newBB = oMapper.convertValue(map, BookingBucket.class);
        return newBB;
    }

    public BookingBucket(String day, String bookingId, List<String> previousBookingOrder, int priority)
    {
        if (day != null)
        {
            this.day = day.replace("-", "");
        }
        this.andBookingsIds = new ArrayList<>();
        this.andBookingsIds.add(bookingId);

        this.minusBookingsIds = new ArrayList<>();
        this.minusBookingsIds.addAll(previousBookingOrder);

        this.priority = priority;
        this.allocatedAmounts = new HashMap<>();
        this.avgTbrMap = new HashMap<>();
    }

    public BookingBucket(String id, Map source)
    {
        this.id = id;
        day = (String) source.get("day");
        minusBookingsIds = (List<String>) source.get("minus_bk_ids");
        andBookingsIds = (List<String>) source.get("and_bk_ids");
        allocatedAmounts = (Map<String, Double>) source.get("allocated_amount");
        priority = (Integer) source.get("priority");
        if (source.containsKey("avg_tbr_map"))
        {
            avgTbrMap = (Map<String, List<Double>>) source.get("avg_tbr_map");
        }
        else
        {
            this.avgTbrMap = new HashMap<>();
        }
    }

    public String getId()
    {
        return id;
    }

    public String getDay()
    {
        return day;
    }

    public Integer getPriority()
    {
        return priority;
    }

    public Map<String, Double> getAllocatedAmounts()
    {
        return allocatedAmounts;
    }

    @JsonIgnore
    public double getSumOfAllocatedAmounts()
    {
        double result = 0;
        for (Number d : this.getAllocatedAmounts().values())
        {
            result += d.doubleValue();
        }
        return result;
    }

    @Override
    public int compareTo(Object o)
    {
        BookingBucket bb = (BookingBucket) o;
        return bb.priority.compareTo(this.priority);
    }

    public Map<String, List<Double>> getAvgTbrMap()
    {
        return avgTbrMap;
    }

    public void setAvgTbrMap(Map<String, List<Double>> avgTbrMap)
    {
        this.avgTbrMap = avgTbrMap;
    }

    public List<String> getAndBookingsIds()
    {
        return andBookingsIds;
    }

    public List<String> getMinusBookingsIds()
    {
        return minusBookingsIds;
    }

    public AvrTBRInsight getAvrTBRInsight(Set<String> validBookingIds)
    {
        double nominator = 0;
        double denominator = 0;
        Impression maxImpression = new Impression();

        Map<String, List<Double>> bbAvgTBR = getAvgTbrMap();
        for (Map.Entry<String, List<Double>> entry : bbAvgTBR.entrySet())
        {
            String bkId = entry.getKey();
            if (validBookingIds.contains(bkId))
            {
                List<Double> values = entry.getValue();
                double tbr = values.get(1);
                nominator += values.get(0) * tbr;
                denominator += values.get(0);

                Impression _impression = new Impression(values.subList(2, 6).toArray(new Double[0]));
                _impression = Impression.multiply(_impression, tbr);
                if (_impression.getTotal() > maxImpression.getTotal())
                {
                    maxImpression = _impression;
                }
            }
        }

        final double _nominator = nominator;
        final double _denominator = denominator;
        final Impression _maxImpression = maxImpression;

        AvrTBRInsight avrTBRInsight = new AvrTBRInsight()
        {
            @Override
            public double getNominator()
            {
                return _nominator;
            }

            @Override
            public double getDenominator()
            {
                return _denominator;
            }

            @Override
            public Impression getMaxImpression()
            {
                return _maxImpression;
            }
        };

        return avrTBRInsight;
    }

}
