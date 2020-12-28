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

package com.bluemarlin.pfservice.model;

import com.bluemarlin.pfservice.util.BMLogger;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;

import java.io.Serializable;
import java.util.Map;

public class Impression implements Serializable
{
    private static final BMLogger LOGGER = BMLogger.instance();
    public static final int TOTAL_NUM_OF_PRICE_CATEGORIES = 4;

    @JsonProperty("h0")
    private long h0;

    @JsonProperty("h1")
    private long h1;

    @JsonProperty("h2")
    private long h2;

    @JsonProperty("h3")
    private long h3;

    public Impression()
    {

    }

    public Impression(Map<String, Aggregation> map)
    {
        for (Map.Entry<String, Aggregation> entry : map.entrySet())
        {
            long value = (long) (((ParsedSum) entry.getValue()).getValue());
            switch (entry.getKey())
            {
                case "h0":
                    this.h0 = value;
                    break;
                case "h1":
                    this.h1 = value;
                    break;
                case "h2":
                    this.h2 = value;
                    break;
                case "h3":
                    this.h3 = value;
                    break;
            }
        }
    }

    public long getCountByPriceCategory(int priceCategory)
    {
        long t = 0;
        switch (priceCategory)
        {
            case 0:
                t = h0;
                break;
            case 1:
                t = h1 + h0;
                break;
            case 2:
                t = h2 + h1 + h0;
                break;
            case 3:
                t = h3 + h2 + h1 + h0;
                break;
            default:
                return t;
        }
        return t;
    }

}
