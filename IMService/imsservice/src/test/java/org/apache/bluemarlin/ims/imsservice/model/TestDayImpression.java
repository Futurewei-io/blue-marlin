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
import java.util.List;

import static org.junit.Assert.*;

public class TestDayImpression
{

    @Test
    public void getDate()
    {
        Double[] dayCnt = new Double[4];
        dayCnt[0] = 0.0;
        dayCnt[1] = 1.1;
        dayCnt[2] = 2.2;
        dayCnt[3] = 3.3;
        DayImpression di = new DayImpression(dayCnt);
        assertNull(di.getDate());
    }

    @Test
    public void setDate()
    {
        Double[] dayCnt = new Double[4];
        dayCnt[0] = 0.0;
        dayCnt[1] = 1.1;
        dayCnt[2] = 2.2;
        dayCnt[3] = 3.3;
        DayImpression di = new DayImpression(dayCnt);
        String day = "20180105";
        di.setDate(day);
        assertEquals(day, di.getDate());
    }

    @Test
    public void getHours()
    {
        Double[] dayCnt = new Double[4];
        dayCnt[0] = 24.0;
        dayCnt[1] = 25.1;
        dayCnt[2] = 26.2;
        dayCnt[3] = 37.3;
        DayImpression di = new DayImpression(dayCnt);

        List<Impression> exp = new ArrayList();
        H0 h0t = new H0(dayCnt[0] / 24.0);
        H1 h1t = new H1(dayCnt[1] / 24.0);
        H2 h2t = new H2(dayCnt[2] / 24.0);
        H3 h3t = new H3(dayCnt[3] / 24.0);
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 24; i++)
        {
            exp.add(imp);
        }
        assertEquals(exp.toString(), di.getHours().toString());
    }

    @Test
    public void setHours()
    {
        Double[] dayCnt = new Double[4];
        dayCnt[0] = 47.0;
        dayCnt[1] = 48.1;
        dayCnt[2] = 49.2;
        dayCnt[3] = 50.3;
        DayImpression di = new DayImpression(dayCnt);
        List<Impression> hours = new ArrayList();
        hours.add(new Impression());
        H0 h0 = new H0(0);
        H1 h1 = new H1(1);
        H2 h2 = new H2(2);
        H3 h3 = new H3(3);
        hours.add(new Impression(h0, h1, h2, h3));
        di.setHours(hours);

        List<Impression> exp = new ArrayList();
        exp.add(new Impression());
        exp.add(new Impression(h0, h1, h2, h3));
        assertEquals(exp.toString(), di.getHours().toString());

        String day = "20180105";
        DayImpression di2 = new DayImpression(day, hours);
        List<Impression> hours2 = new ArrayList();
        hours2.add(new Impression(new H0(6), new H1(7), new H2(8), new H3(9)));
        di2.setHours(hours2);

        exp = new ArrayList();
        exp.add(new Impression(new H0(6), new H1(7), new H2(8), new H3(9)));
        assertEquals(exp.toString(), di2.getHours().toString());
    }

    @Test
    public void initAggregate()
    {
        Double[] dayCnt = new Double[]{24.1, 25.2, 26.3, 27.4};
        DayImpression di = new DayImpression(dayCnt);
        double[] agrCnt = new double[]{19.0, 20.0, 30.0, 40.0};
        di.initAggregate(agrCnt);


        List<Impression> exp = new ArrayList();
        exp.add(new Impression(new H0(19D), new H1(20D), new H2(30D), new H3(40D)));
        H0 h0t = new H0(0);
        H1 h1t = new H1(0);
        H2 h2t = new H2(0);
        H3 h3t = new H3(0);
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 23; i++)
        {
            exp.add(imp);
        }

        assertEquals(exp.toString(), di.getHours().toString());
    }

    @Test
    /**
     * Coverage purpose only. Data not verified.
     */
    public void countImpressions()
    {
        Double[] dayCnt = new Double[]{24.1, 25.2, 26.3, 27.4};
        DayImpression di = new DayImpression(dayCnt);
        double price = 100D;
        TargetingChannel tc = new TargetingChannel();
        tc.setPm("CPC");
        long res = di.countImpressions(price, tc.getPm());
        assertNotNull(res);
        assertEquals(96, res, Double.MAX_VALUE);
        price = Long.MAX_VALUE;
        res = di.countImpressions(price, tc.getPm());
        assertNotNull(res);
        assertEquals(96, res, Double.MAX_VALUE);
    }

    @Test
    public void add()
    {
        Double[] dayCnt = new Double[]{1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7, 8.8};
        DayImpression di = new DayImpression(dayCnt);
        Double[] dayCnt2 = new Double[]{20.5, 20.6, 20.7, 20.8};
        DayImpression di2 = new DayImpression(dayCnt2);
        DayImpression res = DayImpression.add(di, di2);

        Double[] expCnt = new Double[]{21.6, 22.8, 24.0, 25.2};
        DayImpression exp = new DayImpression(expCnt);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void buildEmptyDayImpression()
    {
        DayImpression res = DayImpression.buildEmptyDayImpression("2018-01-05");

        DayImpression exp = new DayImpression();
        exp.setDate("2018-01-05");
        List<Impression> hrs = new ArrayList();
        H0 h0t = new H0(0);
        H1 h1t = new H1(0);
        H2 h2t = new H2(0);
        H3 h3t = new H3(0);
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 24; i++)
        {
            hrs.add(imp);
        }
        exp.setHours(hrs);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void subtract()
    {
        Double[] dayCnt = new Double[]{49D, 60D, 23D, 56D};
        DayImpression di = new DayImpression(dayCnt);
        Double[] dayCnt2 = new Double[]{1D, 2D, 3D, 4D};
        DayImpression di2 = new DayImpression(dayCnt2);
        DayImpression res = DayImpression.subtract(di, di2);

        List<Impression> hrs = new ArrayList();
        H0 h0a = new H0(49D / 24.0), h0b = new H0(1D / 24.0);
        H1 h1a = new H1(60D / 24.0), h1b = new H1(2D / 24.0);
        H2 h2a = new H2(23D / 24.0), h2b = new H2(3D / 24.0);
        H3 h3a = new H3(56D / 24.0), h3b = new H3(4D / 24.0);
        H0 h0t = new H0(h0a.getT() - h0b.getT());
        H1 h1t = new H1(h1a.getT() - h1b.getT());
        H2 h2t = new H2(h2a.getT() - h2b.getT());
        H3 h3t = new H3(h3a.getT() - h3b.getT());
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 24; i++)
        {
            hrs.add(imp);
        }
        DayImpression exp = new DayImpression();
        exp.setHours(hrs);

        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void multiply()
    {
        Double[] dayCnt = new Double[]{10D, 20D, 30D, 40D};
        DayImpression di = new DayImpression(dayCnt);
        DayImpression res = DayImpression.multiply(di, 2);
        System.out.println("multiply " + res);

        List<Impression> hrs = new ArrayList();
        H0 h0a = new H0(10D / 24.0), h0t = new H0(h0a.getT() * 2);
        H1 h1a = new H1(20D / 24.0), h1t = new H1(h1a.getT() * 2);
        H2 h2a = new H2(30D / 24.0), h2t = new H2(h2a.getT() * 2);
        H3 h3a = new H3(40D / 24.0), h3t = new H3(h3a.getT() * 2);
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 24; i++)
        {
            hrs.add(imp);
        }
        DayImpression exp = new DayImpression();
        exp.setHours(hrs);
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void testToString()
    {
        Double[] dayCnt = new Double[]{10D, 20D, 30D, 40D};
        DayImpression di = new DayImpression(dayCnt);
        List<Impression> hrs = new ArrayList();
        H0 h0t = new H0(10D / 24.0);
        H1 h1t = new H1(20D / 24.0);
        H2 h2t = new H2(30D / 24.0);
        H3 h3t = new H3(40D / 24.0);
        Impression imp = new Impression(h0t, h1t, h2t, h3t);
        for (int i = 0; i < 24; i++)
        {
            hrs.add(imp);
        }
        DayImpression exp = new DayImpression();
        exp.setHours(hrs);
        assertEquals(exp.toString(), di.toString());
    }
}