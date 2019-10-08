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

package com.bluemarlin.ims.imsservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;

public class IMSBookingRequest implements Serializable
{
    @JsonProperty("targetingChannel")
    private TargetingChannel targetingChannel;

    @JsonProperty("price")
    private double price;

    @JsonProperty("advId")
    private String advID;

    @JsonProperty
    private List<Range> days;

    @JsonProperty("requestCount")
    private Integer requestCount;

    public TargetingChannel getTargetingChannel()
    {
        return targetingChannel;
    }

    public void setTargetingChannel(TargetingChannel targetingChannel)
    {
        this.targetingChannel = targetingChannel;
    }

    public double getPrice()
    {
        return price;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    public List<Range> getDays()
    {
        return days;
    }

    public void setDays(List<Range> days)
    {
        this.days = days;
    }

    public Integer getRequestCount()
    {
        return requestCount;
    }

    public void setRequestCount(Integer requestCount)
    {
        this.requestCount = requestCount;
    }

    public String getAdvID()
    {
        return advID;
    }

    public void setAdvID(String advID)
    {
        this.advID = advID;
    }
}
