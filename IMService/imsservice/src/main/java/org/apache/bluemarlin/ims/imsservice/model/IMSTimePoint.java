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

import org.apache.bluemarlin.ims.imsservice.util.IMSLogger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class IMSTimePoint implements Comparable<IMSTimePoint>
{
    /**
     * Format is YYYY-MM-DD
     */
    private String date;
    private int hourIndex;

    private static final IMSLogger LOGGER = IMSLogger.instance();
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    /**
     * Format is YYYY-MM-DD-HH
     * 2018-01-01-00
     *
     * @return
     */
    public String toString()
    {
        return "" + this.date + "-" + this.hourIndex;
    }

    public IMSTimePoint(long epochTime)
    {
        SimpleDateFormat formatterDate = new SimpleDateFormat(DATE_FORMAT);
        SimpleDateFormat formatterHour = new SimpleDateFormat("HH");
        String imsDate = formatterDate.format(new Date(epochTime));
        String imsHourIndex = formatterHour.format(new Date(epochTime));
        this.date = imsDate;
        this.hourIndex = Integer.parseInt(imsHourIndex);
    }

    public String getDate()
    {
        return date;
    }

    public void setDate(String date)
    {
        this.date = date;
    }

    public int getHourIndex()
    {
        return hourIndex;
    }

    public void setHourIndex(int hourIndex)
    {
        this.hourIndex = hourIndex;
    }

    public static IMSTimePoint incrementHour(IMSTimePoint imsTimePoint)
    {

        IMSTimePoint localIMSTimePoint = imsTimePoint;

        if (localIMSTimePoint.hourIndex < 23 && localIMSTimePoint.hourIndex > 0)
        {
            ++localIMSTimePoint.hourIndex;
        }
        else
        {
            DateFormat format = new SimpleDateFormat(DATE_FORMAT);
            LocalDateTime dtOrg, dtPlusOne = null;
            DateTimeFormatter outputFormat = null;
            Date localDate;

            try
            {
                localDate = format.parse(imsTimePoint.getDate());
                dtOrg = LocalDateTime.ofInstant(localDate.toInstant(), ZoneId.systemDefault());
                dtPlusOne = dtOrg.plusDays(1);
                outputFormat = DateTimeFormatter.ofPattern(DATE_FORMAT);
                outputFormat.format(dtPlusOne);

            }
            catch (ParseException e)
            {
                LOGGER.error(e.getMessage());
            }

            localIMSTimePoint.setHourIndex(0);
            if (outputFormat != null)
            {
                localIMSTimePoint.setDate(outputFormat.format(dtPlusOne));
            }
        }

        return localIMSTimePoint;
    }

    public static IMSTimePoint incrementDay(IMSTimePoint imsTimePoint)
    {
        IMSTimePoint localIMSTimePoint = imsTimePoint;

        LocalDate dtOrg, dtPlusOne;
        DateTimeFormatter format = DateTimeFormatter.ofPattern(DATE_FORMAT);
        dtOrg = LocalDate.parse(localIMSTimePoint.getDate(), format);
        DateTimeFormatter outputFormat = DateTimeFormatter.ofPattern(DATE_FORMAT);
        dtPlusOne = dtOrg.plusDays(1);
        localIMSTimePoint.setDate(outputFormat.format(dtPlusOne));

        return localIMSTimePoint;
    }

    public static List<IMSTimePoint> build(List<Range> ranges)
    {
        List<IMSTimePoint> imsTimePointList = new ArrayList<>();
        IMSTimePoint localIMSTimePoint;

        DateTimeFormatter formatDate = DateTimeFormatter.ofPattern(DATE_FORMAT);

        LocalDate startlocaldate;
        LocalDate endlocaldate;
        LocalTime startlocalhour;
        LocalTime endlocalhour;

        long startDateTimeEpochMillis;

        //parse from st to ed, sh to eh
        for (Range elt : ranges)
        {
            startlocaldate = LocalDate.parse(elt.getSt(), formatDate);
            startlocalhour = LocalTime.of(Integer.parseInt(elt.getSh()), 0, 0, 0);

            endlocaldate = LocalDate.parse(elt.getEd(), formatDate);
            endlocalhour = LocalTime.of(Integer.parseInt(elt.getEh()), 0, 0, 0);

            LocalDateTime localStartDateTime = LocalDateTime.of(startlocaldate, startlocalhour);
            startDateTimeEpochMillis = localStartDateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

            LocalDateTime localEndDateTime = LocalDateTime.of(endlocaldate, endlocalhour);
            localIMSTimePoint = new IMSTimePoint(startDateTimeEpochMillis);
            imsTimePointList.add(localIMSTimePoint);

            while (localStartDateTime.isBefore(localEndDateTime))
            {
                localStartDateTime = localStartDateTime.plusHours(1);
                startDateTimeEpochMillis = localStartDateTime.
                        atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                localIMSTimePoint = new IMSTimePoint(startDateTimeEpochMillis);
                imsTimePointList.add(localIMSTimePoint);
            }
        }

        return imsTimePointList;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((date == null) ? 0 : date.hashCode());
        result = prime * result + ((Integer.valueOf(hourIndex) == null) ? 0 : Integer.valueOf(hourIndex).hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        IMSTimePoint other = (IMSTimePoint) obj;
        if (date == null)
        {
            if (other.date != null)
                return false;
        }
        else if (!date.equals(other.date))
            return false;
        if (Integer.valueOf(hourIndex) == null)
        {
            if (Integer.valueOf(((IMSTimePoint) obj).hourIndex) != null)
                return false;
        }
        else if (!Integer.valueOf(hourIndex).equals(Integer.valueOf(((IMSTimePoint) obj).hourIndex)))
            return false;
        return true;
    }

    public static List<IMSTimePoint> appendListFromStartToEndHours(int start, int end, List<IMSTimePoint> imsTimePointList, Range elt)
    {

        while (start <= end)
        {
            IMSTimePoint localTempIMSTimePoint = new IMSTimePoint(0);
            localTempIMSTimePoint.setDate(elt.getEd());
            localTempIMSTimePoint.setHourIndex(start);
            imsTimePointList.add(localTempIMSTimePoint);
            ++start;
        }
        return imsTimePointList;
    }

    public LocalDateTime getDateTime()
    {

        LocalDate currentDate = LocalDate.parse(date);
        LocalTime currentTime = LocalTime.of(hourIndex, 0, 0, 0);

        LocalDateTime localDateTime = LocalDateTime.of(currentDate, currentTime);

        return localDateTime;
    }

    @Override
    public int compareTo(IMSTimePoint o)
    {
        if (getDateTime() == null || o.getDateTime() == null)
            return 0;
        return getDateTime().compareTo(o.getDateTime());
    }

}
