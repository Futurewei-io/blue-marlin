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

import org.apache.bluemarlin.ims.imsservice.model.IMSTimePoint;
import org.apache.bluemarlin.ims.imsservice.model.Range;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestIMSTimePoint
{
    @Test
    public void testBuild(){
        Range range = new Range("2018-11-02", "2018-11-03", "7", "7");
        List<Range> ranges=new ArrayList<>();
        ranges.add(range);
        List<IMSTimePoint> imsTimePointList=IMSTimePoint.build(ranges);

        assertEquals("Result", 25, IMSTimePoint.build(ranges).size());
    }
    @Test
    public void testIncrementDay(){
        IMSTimePoint imsTimePoint = new IMSTimePoint(0);
        imsTimePoint.setHourIndex(2);
        imsTimePoint.setDate("2018-08-28");

        assertEquals("Result", "2018-08-29", IMSTimePoint.incrementDay(imsTimePoint).getDate());
    }
    @Test
    public void testIncrementHour(){

        IMSTimePoint imsTimePoint = new IMSTimePoint(0);
        imsTimePoint.setHourIndex(2);
        imsTimePoint.setDate("2018-08-28");

        assertEquals("Result", 3, IMSTimePoint.incrementHour(imsTimePoint).getHourIndex());
    }

}
