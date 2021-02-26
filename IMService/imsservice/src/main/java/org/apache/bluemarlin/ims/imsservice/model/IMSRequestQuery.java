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
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.util.RequestDataConverter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class IMSRequestQuery implements Serializable
{

    @JsonProperty("targetingChannel")
    private TargetingChannel targetingChannel;

    @JsonProperty("price")
    private double price = BookingService.HIGH_PRICE;

    @JsonProperty("days")
    private List<Range> days;

    @JsonProperty("targetingChannel")
    public TargetingChannel getTargetingChannel()
    {
        return targetingChannel;
    }

    @JsonProperty("targetingChannel")
    public void setTargetingChannel(TargetingChannel targetingChannel)
    {
        residenceCityNameConverter(targetingChannel);
        ipCityCodeConverter(targetingChannel);
        this.targetingChannel = targetingChannel;
    }

    @JsonProperty("today")
    private String today;

    public void setToday(String today)
    {
        this.today = today;
    }

    public String getToday()
    {
        return today;
    }

    public double getPrice()
    {
        return price;
    }

    public List<Range> getDays()
    {
        return days;
    }

    public void setDays(List<Range> days)
    {
        this.days = days;
    }

    public void setPrice(double price)
    {
        this.price = price;
    }

    @Override
    public String toString()
    {
        String result = "{" +
                "targetingChannel=" + getTargetingChannel().toString() +
                ",price=" + price +
                ",ranges=";

        for (Range range : getDays())
        {
            result += range.toString();
        }

        result += "}";

        return result;
    }

    private void residenceCityNameConverter(TargetingChannel targetingChannel)
    {
        if (targetingChannel.getR() != null)
        {
            List<String> newResidenceList = new ArrayList<>();
            for (String residence : targetingChannel.getR())
            {
                String region = RequestDataConverter.getRegionByCityName(residence);
                if (!CommonUtil.isBlank(region))
                {
                    newResidenceList.add(region);
                }
            }
            if (newResidenceList.isEmpty())
            {
                newResidenceList.add("_none");
            }

            targetingChannel.setResidenceCityNames(targetingChannel.getR());
            targetingChannel.setR(newResidenceList);
        }
    }

    private void ipCityCodeConverter(TargetingChannel targetingChannel)
    {
        if (targetingChannel.getIpl() != null)
        {
            List<String> newList = new ArrayList<>();
            for (String cityCode : targetingChannel.getIpl())
            {
                String region = RequestDataConverter.getRegionByCityCode(cityCode);
                if (!CommonUtil.isBlank(region))
                {
                    newList.add(region);
                }
            }
            if (newList.isEmpty())
            {
                newList.add("_none");
            }
            targetingChannel.setIplCityCodes(targetingChannel.getIpl());
            targetingChannel.setIpl(newList);
        }
    }

}
