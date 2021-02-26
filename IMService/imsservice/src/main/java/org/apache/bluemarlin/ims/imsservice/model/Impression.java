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

package org.apache.bluemarlin.ims.imsservice.model;

import org.apache.bluemarlin.ims.imsservice.service.BookingService;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.util.IMSLogger;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.Math.min;

public class Impression implements Serializable
{
    static public class ImpressionShort
    {
        @JsonProperty("h0")
        private long h0;

        @JsonProperty("h1")
        private long h1;

        @JsonProperty("h2")
        private long h2;

        @JsonProperty("h3")
        private long h3;

        public ImpressionShort(Impression impression){
            this.h0 = impression.h0.t;
            this.h1 = impression.h1.t;
            this.h2 = impression.h2.t;
            this.h3 = impression.h3.t;
        }
    }
    private static final IMSLogger LOGGER = IMSLogger.instance();

    public static final int TOTAL_NUM_OF_PRICE_CATEGORIES = 4;

    @JsonProperty("h0")
    private H0 h0;

    @JsonProperty("h1")
    private H1 h1;

    @JsonProperty("h2")
    private H2 h2;

    @JsonProperty("h3")
    private H3 h3;

    @JsonProperty("h_index")
    private Integer h_index;

    @JsonProperty("total")
    private Double total;

    public Long getTotal()
    {
        return h0.getT() + h1.getT() + h2.getT() + h3.getT();
    }

    public Impression()
    {
        this.h0 = new H0();
        this.h1 = new H1();
        this.h2 = new H2();
        this.h3 = new H3();
    }

    public Impression(H0 h0, H1 h1, H2 h2, H3 h3)
    {
        this.h0 = h0;
        this.h1 = h1;
        this.h2 = h2;
        this.h3 = h3;
    }

    public Impression(Double[] dayCounts)
    {
        this.h0 = new H0(dayCounts[0]);
        this.h1 = new H1(dayCounts[1]);
        this.h2 = new H2(dayCounts[2]);
        this.h3 = new H3(dayCounts[3]);
    }

    public Impression(double[] dayCounts)
    {
        this.h0 = new H0(dayCounts[0]);
        this.h1 = new H1(dayCounts[1]);
        this.h2 = new H2(dayCounts[2]);
        this.h3 = new H3(dayCounts[3]);
    }

    public Impression(Map map)
    {
        this.h0 = new H0(map.get("h0"));
        this.h1 = new H1(map.get("h1"));
        this.h2 = new H2(map.get("h2"));
        this.h3 = new H3(map.get("h3"));
        this.total = Double.valueOf(map.get("total").toString());
    }

    public double getValueForHIndex(int h_index)
    {
        switch (h_index)
        {
            case 0:
                return this.h0.getT();
            case 1:
                return this.h1.getT();
            case 2:
                return this.h2.getT();
            case 3:
                return this.h3.getT();
            default:
                return 0;
        }
    }

    public long countImpressions(double price, TargetingChannel.PriceModel priceModel)
    {
        long result = 0;
        if (price == BookingService.HIGH_PRICE)
        {
            result = this.getTotal();
        } else
        {
            int priceCategory = CommonUtil.determinePriceCat(price, priceModel);
            result = getCountByPriceCategory(priceCategory);
        }
        LOGGER.debug("impressions={}", result);
        return result;
    }

    public H1 getH1()
    {
        return h1;
    }

    public void setH1(H1 h1)
    {
        this.h1 = h1;
    }

    public H0 getH0()
    {
        return h0;
    }

    public void setH0(H0 h0)
    {
        this.h0 = h0;
    }

    public H2 getH2()
    {
        return h2;
    }

    public void setH2(H2 h2)
    {
        this.h2 = h2;
    }

    public H3 getH3()
    {
        return h3;
    }

    public void setH3(H3 h3)
    {
        this.h3 = h3;
    }

    public Integer getH_index()
    {
        return h_index;
    }

    public void setH_index(Integer h_index)
    {
        this.h_index = h_index;
    }

    public void adjustPositive()
    {
        if (this.h0.getT() < 0)
        {
            this.h0 = new H0(0L);
        }
        if (this.h1.getT() < 0)
        {
            this.h1 = new H1(0L);
        }
        if (this.h2.getT() < 0)
        {
            this.h2 = new H2(0L);
        }
        if (this.h3.getT() < 0)
        {
            this.h3 = new H3(0L);
        }
    }

    public List<Double> getHs()
    {
        List<Double> result = new ArrayList<>();
        result.add(Double.valueOf(this.h0.getT()));
        result.add(Double.valueOf(this.h1.getT()));
        result.add(Double.valueOf(this.h2.getT()));
        result.add(Double.valueOf(this.h3.getT()));
        return result;
    }

    public static Impression add(Impression i1, Impression i2)
    {
        Impression item = new Impression();
        item.h0 = new H0(i1.getH0().getT() + i2.getH0().getT());
        item.h1 = new H1(i1.getH1().getT() + i2.getH1().getT());
        item.h2 = new H2(i1.getH2().getT() + i2.getH2().getT());
        item.h3 = new H3(i1.getH3().getT() + i2.getH3().getT());
        return item;
    }

    public static Impression subtract(Impression i1, Impression i2)
    {
        Impression item = new Impression();
        item.h0 = new H0(i1.getH0().getT() - i2.getH0().getT());
        item.h1 = new H1(i1.getH1().getT() - i2.getH1().getT());
        item.h2 = new H2(i1.getH2().getT() - i2.getH2().getT());
        item.h3 = new H3(i1.getH3().getT() - i2.getH3().getT());
        return item;
    }

    public static Impression multiply(Impression i1, double r)
    {
        Impression item = new Impression();
        item.h0 = new H0(i1.getH0().getT() * r);
        item.h1 = new H1(i1.getH1().getT() * r);
        item.h2 = new H2(i1.getH2().getT() * r);
        item.h3 = new H3(i1.getH3().getT() * r);
        return item;
    }

    public static Impression subtractBookedValue(Impression i1, long v)
    {
        Impression item = new Impression();
        long[] impValues = new long[]{i1.getH0().getT(), i1.getH1().getT(), i1.getH2().getT(), i1.getH3().getT()};

        long[] hBooked = new long[4];
        for (int i = 0; i < 4; i++)
        {
            hBooked[0] = min(impValues[i], v);
            v -= hBooked[0];
        }

        item.h0 = new H0(impValues[0] - hBooked[0]);
        item.h1 = new H1(impValues[1] - hBooked[1]);
        item.h2 = new H2(impValues[2] - hBooked[2]);
        item.h3 = new H3(impValues[3] - hBooked[3]);
        return item;
    }

    public long getCountByPriceCategory(int priceCategory)
    {
        long t = 0;
        switch (priceCategory)
        {
            case 0:
                t = h0.getT();
                break;
            case 1:
                t = h1.getT() + h0.getT();
                break;
            case 2:
                t = h2.getT() + h1.getT() + h0.getT();
                break;
            case 3:
                t = h3.getT() + h2.getT() + h1.getT() + h0.getT();
                break;
            default:
                return t;
        }
        return t;
    }

    public long getIndividualHistogramCountByPriceCategory(int priceCategory)
    {
        long t = 0;
        switch (priceCategory)
        {
            case 0:
                t = h0.getT();
                break;
            case 1:
                t = h1.getT();
                break;
            case 2:
                t = h2.getT();
                break;
            case 3:
                t = h3.getT();
                break;
            default:
                return t;
        }
        return t;
    }

    public void adjustValue(int priceCategory, long t)
    {
        switch (priceCategory)
        {
            case 0:
                h0 = new H0(t);
                break;
            case 1:
                h1 = new H1(t);
                break;
            case 2:
                h2 = new H2(t);
                break;
            case 3:
                h3 = new H3(t);
                break;
            default:
                break;
        }
    }

    @Override
    public String toString()
    {
        String result = "";
        result += "{" + getH0().toString() + getH1().toString() + getH2().toString() + getH3().toString() + "}";
        return result;
    }
}
