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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

public class TestDay
{

    @Test
    public void getDayStr()
    {
        String dayStr = "20180105";
        Day day = new Day(dayStr);
        assertEquals("2018-01-05", day.toString());
    }

    @Test
    public void getHours()
    {
        String dayStr = "20180105";
        List<Integer> hrList = new ArrayList();
        hrList.add(0);
        hrList.add(3);
        hrList.add(24);
        Day day = new Day(dayStr, hrList);
        assertEquals(hrList, day.getHours());
    }

    @Test
    public void buildSortedDays()
    {
        //buildSortedDays(List<Range> ranges)
        List<Range> ranges = new ArrayList();
        Range r1 = new Range();
        r1.setSt("2018-01-07");
        r1.setEd("2018-01-07");
        r1.setSh("0");
        r1.setEh("23");
        ranges.add(r1);

        String dayStr = "20180105";
        Day day = new Day(dayStr);

        List<Day> exp = new ArrayList();
        exp.add(new Day("2018-01-07"));
        assertEquals(exp, day.buildSortedDays(ranges));
    }

    @Test
    public void getDayString()
    {
        String dayStr = "20180105", dayStr2 = "20180106", dayStr3 = "20180107";
        Day day = new Day(dayStr), day2 = new Day(dayStr2), day3 = new Day(dayStr3);
        Set<Day> days = new HashSet();
        days.add(day);
        days.add(day2);
        days.add(day3);

        Set<String> exp = new HashSet();
        exp.add("2018-01-05");
        exp.add("2018-01-06");
        exp.add("2018-01-07");
        assertEquals(exp, Day.getDayString(days));
    }

    @Test
    public void getDayStringTreated()
    {
        String dayStr = "20180105", dayStr2 = "20180106", dayStr3 = "20180107";
        Day day = new Day(dayStr), day2 = new Day(dayStr2), day3 = new Day(dayStr3);
        Set<Day> days = new HashSet();
        days.add(day);
        days.add(day2);
        days.add(day3);

        Set<String> exp = new HashSet();
        exp.add("2018-01-05");
        exp.add("2018-01-06");
        exp.add("");

        assertEquals(exp, Day.getDayStringTreated(days, day3.toString()));
    }

    @Test
    public void testHashCode()
    {
        String dayStr = "20180105";
        Day day = new Day(dayStr);
        assertNotNull(day.hashCode());
    }

    @Test
    public void testEquals()
    {
        //equals(Object dayObject)
        String dayStr = "20180105", dayStr2 = "20180106", dayStr3 = "20180107";
        Day day = new Day(dayStr), day2 = new Day(dayStr2), day3 = new Day(dayStr3);
        assertFalse(day.equals(null));
        assertFalse(day.equals(day3));
        assertTrue(day.equals(new Day(dayStr)));
        assertTrue(day.equals(day));
        assertFalse(day2.equals(day3));
    }

    @Test
    public void treatDayStr()
    {
        String dayStr = "20180105";
        assertEquals("2018-01-05", Day.treatDayStr(dayStr));
    }

    @Test
    public void compareTo()
    {
        String dayStr = "20180105", dayStr2 = "20180106", dayStr3 = "20180107";
        Day day = new Day(dayStr), day2 = new Day(dayStr2), day3 = new Day(dayStr3);

        assertEquals(-2, day.compareTo(day3), 0);
        assertEquals(0, day.compareTo(new Day(dayStr)), 0);
        assertEquals(0, day.compareTo(day), 0);
        assertEquals(1, day2.compareTo(day), 0);
    }
}