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

package org.apache.bluemarlin.ims.imsservice.util;

import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestRequestDataConverter
{

    static final Map DEF_CODE_REGION_RATIO_MAP;

    static
    {
        DEF_CODE_REGION_RATIO_MAP = new HashMap();
        DEF_CODE_REGION_RATIO_MAP.put("1156511500", Arrays.asList(80, 0.22363130838317366));
        DEF_CODE_REGION_RATIO_MAP.put("1156430900", Arrays.asList(76, 1D));
        DEF_CODE_REGION_RATIO_MAP.put("1156511700", Arrays.asList(80, 0.2734432820594883));
        DEF_CODE_REGION_RATIO_MAP.put("1156510600", Arrays.asList(80, 0.18081531414398017));
        DEF_CODE_REGION_RATIO_MAP.put("1156511600", Arrays.asList(80, 0.16029744164182988));
        DEF_CODE_REGION_RATIO_MAP.put("1156510700", Arrays.asList(79, 0.4235850355749369));
        DEF_CODE_REGION_RATIO_MAP.put("1156430100", Arrays.asList(3, 1D));
        DEF_CODE_REGION_RATIO_MAP.put("1156230700", Arrays.asList(70, 1D));
        DEF_CODE_REGION_RATIO_MAP.put("1156511100", Arrays.asList(80, 0.161812653771528));
        DEF_CODE_REGION_RATIO_MAP.put("1156511300", Arrays.asList(79, 0.5764149644250631));
    }


    @Test
    public void getRegionByCityCode()
    {
        String city = "1156510600";
        String res = RequestDataConverter.getRegionByCityCode(city);
        assertEquals("80", res);
        city = "123";
        res = RequestDataConverter.getRegionByCityCode(city);
        assertEquals("", res);
    }

    @Test
    public void getRegionByCityName()
    {
        String city = "city1";
        String res = RequestDataConverter.getRegionByCityName(city);
        assertEquals("80", res);
        city = "123";
        res = RequestDataConverter.getRegionByCityName(city);
        assertEquals("", res);
    }

    @Test
    public void getCityCodeByCityName()
    {
        String city = "city3";
        String res = RequestDataConverter.getCityCodeByCityName(city);
        assertEquals("1156511300", res);
        city = "123";
        res = RequestDataConverter.getCityCodeByCityName(city);
        assertEquals("", res);
    }

    @Test
    public void getRegionRatioByCityCode()
    {
        String city = "1156510600";
        Double res = RequestDataConverter.getRegionRatioByCityCode(city);
        assertEquals(0.18081531414398017, res, 0.1);
        city = "123";
        res = RequestDataConverter.getRegionRatioByCityCode(city);
        assertEquals(0D, res, 0D);
    }

    @Test
    public void getRegionRatioMap()
    {
        TargetingChannel tc = new TargetingChannel();
        tc.setIplCityCodes(Arrays.asList("1156510600"));
        Map res = RequestDataConverter.getRegionRatioMap(tc);
        List expRes = (List) DEF_CODE_REGION_RATIO_MAP.get("1156510600");
        Map exp = new HashMap();
        exp.put(String.valueOf(expRes.get(0)), (Double) expRes.get(1));
        assertTrue(res.equals(exp));

        tc = new TargetingChannel();
        tc.setIplCityCodes(Arrays.asList("123"));
        res = RequestDataConverter.getRegionRatioMap(tc);
        exp = new HashMap();
        exp.put("", 0.0);
        assertTrue(res.equals(exp));

        tc = new TargetingChannel();
        tc.setResidenceCityNames(Arrays.asList("city13"));
        res = RequestDataConverter.getRegionRatioMap(tc);
        expRes = (List) DEF_CODE_REGION_RATIO_MAP.get("1156430900");
        exp = new HashMap();
        exp.put(String.valueOf(expRes.get(0)), (Double) expRes.get(1));
        assertTrue(res.equals(exp));
    }

    @Test
    public void getHourDayRatio()
    {
        List<Integer> list = new ArrayList();
        list.add(0);
        list.add(14);
        list.add(23);
        double res = RequestDataConverter.getHourDayRatio(list);
        double exp = (2.905931696 + 4.219093269 + 2.900938686) / 100; //0.10025963651
        assertEquals(exp, res, 0D);

        int[] A = new int[24];
        list = new ArrayList();
        for (int i : A)
        {
            list.add(i);
        }
        res = RequestDataConverter.getHourDayRatio(list);
        assertEquals(1D, res, 0D);

    }

    @Test
    public void buildRatioCityCodeRegionMap()
    {
        List<String> list = new ArrayList();
        list.add("1156511600");
        list.add("1156511700");
        list.add("1156230700");
        list.add("1156430900");
        Map res = RequestDataConverter.buildRatioCityCodeRegionMap(list);
        Map exp = new HashMap();
        List expRes1 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156511600");
        List expRes2 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156511700");
        List expRes3 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156230700");
        List expRes4 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156430900");
        List<String> oneL = new ArrayList(), twoL = new ArrayList();
        oneL.add(String.valueOf(expRes3.get(0)));
        oneL.add(String.valueOf(expRes4.get(0)));
        exp.put(1D, oneL);
        Double sum = (Double) expRes1.get(1) + (Double) expRes2.get(1);
        twoL.add(String.valueOf(expRes1.get(0)));
        exp.put(sum, twoL);

        assertTrue(res.equals(exp));
    }

    @Test
    public void buildRatioResidenceRegionMap()
    {
        List<String> list = new ArrayList();
        list.add("city1");
        list.add("city11");
        list.add("city3");
        list.add("city14");
        list.add("city13");
        Map res = RequestDataConverter.buildRatioResidenceRegionMap(list);

        Map exp = new HashMap();
        List<String> list1 = new ArrayList(), list2 = new ArrayList(), list3 = new ArrayList();
        List res1 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156510600"),
                res2 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156511600"),
                res3 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156511300"),
                res4 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156430100"),
                res5 = (List) DEF_CODE_REGION_RATIO_MAP.get("1156430900");
        list1.add(String.valueOf(res4.get(0)));
        list1.add(String.valueOf(res5.get(0)));
        exp.put(1D, list1);
        exp.put((Double) res3.get(1), Arrays.asList(String.valueOf(res3.get(0))));
        Double sum = (Double) res1.get(1) + (Double) res2.get(1);
        exp.put(sum, Arrays.asList(String.valueOf(res1.get(0))));

        assertTrue(res.equals(exp));
    }
}
