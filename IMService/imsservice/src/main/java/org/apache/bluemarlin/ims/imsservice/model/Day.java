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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final public class Day implements Comparable
{
    private final String dayStr;
    private final List<Integer> hours = new ArrayList<>();

    /**
     * Day format is YYYY-MM-DD
     *
     * @param dayStr
     */
    public Day(String dayStr)
    {
        dayStr = treatDayStr(dayStr);
        this.dayStr = dayStr;
        for (int i = 0; i < 24; i++)
        {
            hours.add(i);
        }
    }

    public Day(String dayStr, List<Integer> hours)
    {
        this.dayStr = dayStr;
        for (Integer i : hours){
            this.hours.add(i.intValue());
        }
    }

    public String toString()
    {
        return dayStr;
    }

    public List<Integer> getHours()
    {
        return hours;
    }

    public static List<Day> buildSortedDays(List<Range> ranges)
    {

        List<IMSTimePoint> imsTimePointList = IMSTimePoint.build(ranges);

        /**
         * Create a map with keys as days and values as list of hours
         */
        Map<String, List<Integer>> timePointsMap = new HashMap<>();
        for (IMSTimePoint imsTimePoint : imsTimePointList)
        {
            String date = imsTimePoint.getDate();
            if (!timePointsMap.containsKey(date))
            {
                timePointsMap.put(date, new ArrayList<>());
            }
            timePointsMap.get(date).add(imsTimePoint.getHourIndex());
        }

        Set<Day> days = new HashSet<>();
        for (Map.Entry<String, List<Integer>> entry : timePointsMap.entrySet())
        {
            Day day = new Day(entry.getKey(), entry.getValue());
            days.add(day);
        }

        List<Day> sortedDays = new ArrayList<>(days);
        Collections.sort(sortedDays);

        return sortedDays;
    }

    public static Set<String> getDayString(Set<Day> days)
    {
        return getDayStringTreated(days, "");
    }

    public static Set<String> getDayStringTreated(Set<Day> days, String toBeRemoved)
    {
        Set<String> result = new HashSet<>();
        for (Day day : days)
        {
            result.add(day.dayStr.replace(toBeRemoved, ""));
        }
        return result;
    }

    @Override
    public int hashCode()
    {
        return dayStr.hashCode();
    }

    @Override
    public boolean equals(Object dayObject)
    {
        if (dayObject == null || dayObject.getClass() != this.getClass()){
            return false;
        }

        Day day = (Day) dayObject;
        return this.dayStr.equals(day.dayStr);
    }

    public static String treatDayStr(String dayStr)
    {
        if (!dayStr.contains("-"))
        {
            dayStr = dayStr.substring(0, 4) + "-" + dayStr.substring(4, 6) + "-" +
                    dayStr.substring(6, 8);
        }
        return dayStr;
    }

    @Override
    public int compareTo(Object o)
    {
        return this.dayStr.compareTo(((Day)o).dayStr);
    }
}
