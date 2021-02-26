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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class Range implements Serializable
{

    private static final long serialVersionUID = -927612992193878704L;

    @JsonProperty("st")
    private String st;

    @JsonProperty("ed")
    private String ed;

    @JsonProperty("eh")
    private String eh;

    @JsonProperty("sh")
    private String sh;

    public Range()
    {
        this.st = null;
        this.ed = null;
        this.sh = null;
        this.eh = null;
    }

    public Range(String st, String ed, String sh, String eh)
    {
        this.st = st;
        this.ed = ed;
        this.sh = sh;
        this.eh = eh;
    }

    /**
     * Return the length of the range in days
     *
     * @return
     */
    public int getLength() throws ParseException
    {
        return getLength(st, ed);
    }

    public static int getLength(String smallerDate, String BiggerDate)
    {
        LocalDate startDate = LocalDate.parse(smallerDate);
        LocalDate endDate = LocalDate.parse(BiggerDate);
        long numberOfDays = ChronoUnit.DAYS.between(startDate, endDate);
        return (int) numberOfDays + 1;
    }

    public String getSt()
    {
        return st;
    }

    public void setSt(String st)
    {
        this.st = st;
    }

    public String getEd()
    {
        return ed;
    }

    public void setEd(String ed)
    {
        this.ed = ed;
    }

    public String getSh()
    {
        return sh;
    }

    public void setSh(String sh)
    {
        this.sh = sh;
    }

    public String getEh()
    {
        return eh;
    }

    public void setEh(String eh)
    {
        this.eh = eh;
    }

    public static List<Range> normalize(List<Range> ranges)
    {
        List<Range> newRanges = new ArrayList<>();
        List<IMSTimePoint> imsTimePoints = IMSTimePoint.build(ranges);
        TreeSet<IMSTimePoint> imsTimePointTreeSet = new TreeSet<>(imsTimePoints);
        Iterator<IMSTimePoint> it = imsTimePointTreeSet.iterator();
        String day = "";
        int sh = -1;
        int eh = -1;
        while (it.hasNext())
        {
            IMSTimePoint imsTimePoint = it.next();
            if (!day.equals(imsTimePoint.getDate()) && !day.equals(""))
            {
                Range newRange = new Range();
                newRange.st = day;
                newRange.ed = day;
                newRange.sh = String.valueOf(sh);
                newRange.eh = String.valueOf(eh);
                newRanges.add(newRange);
                day = "";
                sh = -1;
            }
            if (day.equals(""))
            {
                day = imsTimePoint.getDate();
            }
            if (sh == -1)
            {
                sh = imsTimePoint.getHourIndex();
            }
            eh = imsTimePoint.getHourIndex();
        }

        if (((newRanges.size() > 0 && !day.equals(newRanges.get(newRanges.size() - 1).getSt())) || (newRanges.size() == 0)) && !day.equals(""))
        {
            Range newRange = new Range();
            newRange.st = day;
            newRange.ed = day;
            newRange.sh = String.valueOf(sh);
            newRange.eh = String.valueOf(eh);
            newRanges.add(newRange);
        }

        return newRanges;
    }

    public static List<String> getDays(List<Range> ranges)
    {
        List<Range> normalizedRanges = normalize(ranges);
        Set<String> daysSet = new HashSet<>();
        for (Range range : normalizedRanges)
        {
            daysSet.add(range.st);
        }
        List<String> result = new ArrayList<>(daysSet);
        Collections.sort(result);
        return result;
    }

    @Override
    public String toString()
    {
        return "Range{" +
                "st='" + st +
                ",ed='" + ed +
                ",sh=" + sh +
                ",eh=" + eh +
                '}';
    }

}
