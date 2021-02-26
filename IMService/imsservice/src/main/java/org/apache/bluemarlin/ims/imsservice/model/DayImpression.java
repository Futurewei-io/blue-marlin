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

import org.apache.bluemarlin.ims.imsservice.service.BookingService;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.util.IMSLogger;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DayImpression implements Serializable
{
    private static final IMSLogger LOGGER = IMSLogger.instance();

    @JsonProperty("date")
    private String date;

    @JsonProperty("hours")
    private List<Impression> hours;

    public String getDate()
    {
        return date;
    }

    public void setDate(String date)
    {
        this.date = date;
    }

    public List<Impression> getHours()
    {
        return hours;
    }

    public void setHours(List<Impression> hours)
    {
        this.hours = hours;
    }

    public DayImpression(String date, List<Impression> hours)
    {
        this.date = date;
        this.hours = hours;
    }

    public DayImpression()
    {
    }

    /**
     * If we have day counts one way to get hourly counts is to evenly distribute it.
     * We can also use the day hour distribution map.
     *
     * @param dayCounts
     */
    public DayImpression(Double[] dayCounts)
    {
        this.hours = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            H0 h0t = new H0(dayCounts[0] / 24.0);
            H1 h1t = new H1(dayCounts[1] / 24.0);
            H2 h2t = new H2(dayCounts[2] / 24.0);
            H3 h3t = new H3(dayCounts[3] / 24.0);
            Impression impression = new Impression(h0t, h1t, h2t, h3t);
            hours.add(impression);
        }
    }

    public DayImpression(Object hoursMap)
    {
        this.hours = new ArrayList<>();
        ObjectMapper oMapper = new ObjectMapper();
        List<Map> _hoursMap = oMapper.convertValue(hoursMap, List.class);
        for (Map map : _hoursMap)
        {
            H0 h0t = new H0(map.get("h0t"));
            H1 h1t = new H1(map.get("h1t"));
            H2 h2t = new H2(map.get("h2t"));
            H3 h3t = new H3(map.get("h3t"));
            Impression impression = new Impression(h0t, h1t, h2t, h3t);
            hours.add(impression);
        }
    }

    public void initAggregate(double[] aggregatedHCounts)
    {
        this.hours = new ArrayList<>();
        H0 h0t = new H0(aggregatedHCounts[0]);
        H1 h1t = new H1(aggregatedHCounts[1]);
        H2 h2t = new H2(aggregatedHCounts[2]);
        H3 h3t = new H3(aggregatedHCounts[3]);
        Impression impression = new Impression(h0t, h1t, h2t, h3t);
        hours.add(impression);
        for (int i = 0; i < 23; i++)
        {
            H0 h0tmp = new H0(0);
            H1 h1tmp = new H1(0);
            H2 h2tmp = new H2(0);
            H3 h3tmp = new H3(0);
            Impression impressionTmp =
                    new Impression(h0tmp, h1tmp, h2tmp, h3tmp);
            hours.add(impressionTmp);
        }
    }

    public long countImpressions(double price, TargetingChannel.PriceModel priceModel)
    {
        long result = 0;
        if (price == BookingService.HIGH_PRICE)
        {
            for (Impression h : hours)
            {
                result += h.getTotal();
            }
        }
        else
        {
            int priceCategory = CommonUtil.determinePriceCat(price, priceModel);
            for (Impression h : hours)
            {
                long t = h.getCountByPriceCategory(priceCategory);
                result += t;
            }
        }
        LOGGER.debug("impressions={}", result);
        return result;
    }

    public static DayImpression add(DayImpression d1, DayImpression d2)
    {
        DayImpression d3 = new DayImpression();
        d3.hours = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            Impression i1 = d1.hours.get(i);
            Impression i2 = d2.hours.get(i);
            Impression i3 = Impression.add(i1, i2);
            d3.hours.add(i3);
        }
        LOGGER.debug("dayImpression after add:{}", d3.toString());
        return d3;
    }

    public static DayImpression buildEmptyDayImpression(String date)
    {
        DayImpression dayImpression = new DayImpression();
        dayImpression.setDate(date);
        dayImpression.hours = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            Impression impression = new Impression();
            dayImpression.hours.add(impression);
        }
        return dayImpression;
    }

    public static DayImpression subtract(DayImpression d1, DayImpression d2)
    {
        DayImpression d3 = new DayImpression();
        d3.hours = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            Impression i1 = d1.hours.get(i);
            Impression i2 = d2.hours.get(i);
            Impression i3 = Impression.subtract(i1, i2);
            d3.hours.add(i3);
        }
        LOGGER.debug("dayImpression after subtract:{}", d3.toString());
        return d3;
    }

    public static DayImpression multiply(DayImpression d1, double r)
    {
        DayImpression d3 = new DayImpression();
        d3.hours = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            Impression i1 = d1.hours.get(i);
            Impression i3 = Impression.multiply(i1, r);
            d3.hours.add(i3);
        }
        return d3;
    }

    @Override
    public String toString()
    {
        String result = "{";

        if (getDate() != null)
        {
            result += "date" + getDate();
        }

        result += "hours:";
        List<Impression> hoursList = getHours();
        for (Impression hour : hoursList)
        {
            result += hour.toString();
        }

        result += "}";
        return result;
    }
}
