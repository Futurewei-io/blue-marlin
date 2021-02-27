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

package com.bluemarlin.pfservice.util;

import com.bluemarlin.pfservice.model.Traffic;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class CommonUtil
{
    private CommonUtil()
    {
        throw new IllegalStateException("This is utility class.");
    }

    /**
     * This method returns false if a list is null or empty.
     *
     * @param list
     * @return
     */
    public static boolean isEmpty(List list)
    {
        if (list == null || list.size() == 0)
            return true;
        return false;
    }

    /**
     * This method returns false if a string length is 0 or string is null.
     *
     * @param str
     * @return
     */
    public static boolean isBlank(String str)
    {
        if (str == null || str.length() == 0)
            return true;
        return false;
    }

    /**
     * This method trims string and converts it to lowercase.
     *
     * @param v
     * @return
     */
    public static String sanitize(String v)
    {
        if (v != null)
        {
            return v.toLowerCase().trim();
        }
        return null;
    }

    public static String DateToDayString(Date date)
    {
        String pattern = "yyyy-MM-dd";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        return simpleDateFormat.format(date);
    }

    public static Date dayToDate(String day) throws ParseException
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = format.parse(day);
        return date;
    }

    /**
     * This method converts a list to a string by joining its elements using '-' separator.
     *
     * @param list
     * @return
     */
    public static String convertToString(List<String> list)
    {
        StringBuilder _sb = new StringBuilder();
        for (String item : list)
        {
            if (item != null)
            {
                _sb.append(item.toLowerCase().trim());
            }
            _sb.append("-");
        }
        int _length = _sb.length();
        if (_length > 0)
        {
            _sb.delete(_length - 1, _length);
        }
        return _sb.toString();
    }

    /**
     * This method converts map-like object into POJO map.
     *
     * @param object
     * @return
     */
    public static Map convertToMap(Object object)
    {
        ObjectMapper oMapper = new ObjectMapper();
        Map<String, Object> map = oMapper.convertValue(object, Map.class);
        return map;
    }

    /**
     * This method replaces a substring in every string item of the list with empty string.
     *
     * @param list
     * @param toRemove
     * @return
     */
    public static List<String> removeStringFromItems(List<String> list, String toRemove)
    {
        List<String> r = new ArrayList<>();
        if (list != null)
        {
            for (String item : list)
            {
                if (item != null)
                {
                    r.add(item.replace(toRemove, ""));
                }
            }
        }
        return r;
    }

    public static Integer determinePriceCatForCPM(double price)
    {
        if (price >= 0.0 && price <= 12)
        {
            return 1;
        }
        if (price > 12 && price <= 25)
        {
            return 2;
        }
        if (price > 25)
        {
            return 3;
        }
        return 3;
    }

    public static Integer determinePriceCatForCPC(double price)
    {
        if (price >= 0.0 && price <= 0.7)
        {
            return 1;
        }
        if (price > 0.7 && price <= 2)
        {
            return 2;
        }
        if (price > 2)
        {
            return 3;
        }
        return 3;
    }

    public static Integer determinePriceCatForCPD(double price)
    {
        if (price >= 0 && price <= 3)
        {
            return 1;
        }
        if (price > 3 && price <= 5)
        {
            return 2;
        }
        if (price > 5)
        {
            return 3;
        }
        return 3;
    }

    public static Integer determinePriceCatForCPT(double price)
    {
        if (price >= 0 && price <= 60000)
        {
            return 1;
        }
        if (price > 60000 && price <= 400000)
        {
            return 2;
        }
        if (price > 400000)
        {
            return 3;
        }
        return 3;
    }

    /**
     * This method returns highest price category for different pricing models.
     *
     * @param price
     * @param priceModel
     * @return
     */
    public static Integer determinePriceCat(double price, Traffic.PriceModel priceModel)
    {
        if (price < 0)
        {
            return 0;
        }

        if (priceModel == Traffic.PriceModel.CPM)
        {
            return determinePriceCatForCPM(price);
        } else if (priceModel == Traffic.PriceModel.CPC)
        {
            return determinePriceCatForCPC(price);
        } else if (priceModel == Traffic.PriceModel.CPD)
        {
            return determinePriceCatForCPD(price);
        } else if (priceModel == Traffic.PriceModel.CPT)
        {
            return determinePriceCatForCPT(price);
        }
        return 3;
    }

    /**
     * This method removes escaping characters of object.toString()
     *
     * @param str
     * @return
     */
    public static String removeEscapingCharacters(String str)
    {
        return str.replaceAll("\"", "").replaceAll("\n", "").replaceAll("&apos;", "").replaceAll("&lt", "").replaceAll("&gt", "");
    }

    public static boolean equalNumbers(double d1, double d2)
    {
        if (Math.abs(d1 - d2) <= 0.001)
        {
            return true;
        }
        return false;
    }

}
