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

import java.util.*;

import static org.junit.Assert.*;

public class TestTargetingChannel
{

    @Test
    public void addAllAttributeValues()
    {

        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));

        TargetingChannel tc2 = new TargetingChannel();
        tc2.setG(Arrays.asList("g_m"));

        TargetingChannel tc3 = new TargetingChannel();
        tc3.setA(Arrays.asList("2"));

        TargetingChannel tc4 = new TargetingChannel();

        List<TargetingChannel> tcs = new ArrayList();
        tcs.add(tc);
        tcs.add(tc2);
        tcs.add(tc3);
        tcs.add(tc4);

        String atrName = "gender";
        List<String> exp = new ArrayList();
        exp.add(tc.toString());
        exp.add(tc2.toString());
        exp.add(tc3.toString());
        exp.add(tc4.toString());
        assertEquals(exp.toString(), tcs.toString());

        List<String> res = TargetingChannel.addAllAttributeValues(tcs, atrName);
        exp = new ArrayList();
        exp.addAll(tc2.getG());
        exp.addAll(tc.getG());
        assertEquals(exp.toString(), res.toString());

        atrName = "residence";
        res = TargetingChannel.addAllAttributeValues(tcs, atrName);
        exp = new ArrayList();
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void build()
    {
        Map<String, Object> map = new HashMap();
        map.put("gender", "g_f");
        TargetingChannel res = TargetingChannel.build(map);
        TargetingChannel exp = new TargetingChannel();
        exp.setG(Arrays.asList("g_f"));
        assertEquals(exp.toString(), res.toString());

        map = new HashMap();
        map.put("pm", "CPC");
        res = TargetingChannel.build(map);
        exp = new TargetingChannel();
        exp.setPm("CPC");
        assertEquals(exp.toString(), res.toString());

        map = new HashMap();
        List<Integer> age = new ArrayList();
        age.add(0);
        age.add(2);
        map.put("age", age);
        res = TargetingChannel.build(map);

        exp = new TargetingChannel();
        exp.setA(Arrays.asList("0", "2"));
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void getAttributeValue()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        tc.setAttributeValue("g", val);
        List<String> resA = tc.getAttributeValue("age"),
                resG = tc.getAttributeValue("gender"),
                resM = tc.getAttributeValue("media"),
                resS = tc.getAttributeValue("si"),
                resC = tc.getAttributeValue("connectionType"),
                resX = tc.getAttributeValue("xxx"),
                resN = tc.getAttributeValue("");

        assertEquals(val.toString(), resG.toString());
        assertNull(resA);
        assertNull(resM);
        assertNull(resS);
        assertNull(resC);
        assertTrue(resX.isEmpty());
        assertTrue(resN.isEmpty());
    }

    @Test
    public void setAttributeValue()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        tc.setAttributeValue("g", val);
        List<String> resA = tc.getAttributeValue("age"),
                resG = tc.getAttributeValue("gender"),
                resM = tc.getAttributeValue("media");
        assertNull(resA);
        assertEquals(val.toString(), resG.toString());
        assertNull(resM);

        tc = new TargetingChannel();
        val = new ArrayList();
        val.add("123");
        val.add("456");
        tc.setAttributeValue("a", val);
        resA = tc.getAttributeValue("age");
        resG = tc.getAttributeValue("gender");
        resM = tc.getAttributeValue("media");
        assertEquals(val.toString(), resA.toString());
        assertNull(resM);
        assertNull(resG);

        tc = new TargetingChannel();
        val = new ArrayList();
        val.add("123");
        val.add("456");
        tc.setAttributeValue("m", val);
        resA = tc.getAttributeValue("age");
        resG = tc.getAttributeValue("gender");
        resM = tc.getAttributeValue("media");
        assertNull(resM);
        assertNull(resG);
        assertNull(resA);
    }

    @Test
    public void getAis()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setAis(val);
        List<String> res = tc.getAis();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void setAis()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setAis(val);
        List<String> res = tc.getAis();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void getPdas()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setPreDefinedAudience(val);
        List<String> res = tc.getPdas();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setPreDefinedAudience()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setPreDefinedAudience(val);
        List<String> res = tc.getPdas();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getDms()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setDeviceModel(val);
        List<String> res = tc.getDms();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setDeviceModel()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setDeviceModel(val);
        List<String> res = tc.getDms();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getAus()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setAus(val);
        List<String> res = tc.getAus();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void setAus()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setAus(val);
        List<String> res = tc.getAus();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void setDms()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setDeviceModel(val);
        List<String> res = tc.getDms();
        assertEquals(val.toString(), res.toString());
        tc = new TargetingChannel();
        tc.setDms(val);
        res = tc.getDms();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void setPdas()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setPreDefinedAudience(val);
        List<String> res = tc.getPdas();
        assertEquals(val.toString(), res.toString());
        tc = new TargetingChannel();
        tc.setPdas(val);
        res = tc.getPdas();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getR()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setR(val);
        List<String> res = tc.getR();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setR()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setR(val);
        List<String> res = tc.getR();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getG()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setG(val);
        List<String> res = tc.getG();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setG()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setG(val);
        List<String> res = tc.getG();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getA()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setA(val);
        List<String> res = tc.getA();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setA()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setA(val);
        List<String> res = tc.getA();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getT()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setT(val);
        List<String> res = tc.getT();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setT()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setT(val);
        List<String> res = tc.getT();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getSi()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setSi(val);
        List<String> res = tc.getSi();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setSi()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setSi(val);
        List<String> res = tc.getSi();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getM()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setM(val);
        List<String> res = tc.getM();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setM()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setM(val);
        List<String> res = tc.getM();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getDpc()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setDpc(val);
        List<String> res = tc.getDpc();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setDpc()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setDpc(val);
        List<String> res = tc.getDpc();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getIpl()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setIpl(val);
        List<String> res = tc.getIpl();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setIpl()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setIpl(val);
        List<String> res = tc.getIpl();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getExclude_pdas()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setExclude_pdas(val);
        List<String> res = tc.getExclude_pdas();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setExclude_pdas()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setExclude_pdas(val);
        List<String> res = tc.getExclude_pdas();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getApps()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setApps(val);
        List<String> res = tc.getApps();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void setApps()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setApps(val);
        List<String> res = tc.getApps();
        List<String> exp = new ArrayList();
        exp.add("123");
        exp.add("456");
        exp.add("789");
        assertEquals(exp.toString(), res.toString());
    }

    @Test
    public void getExclude_apps()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setExclude_apps(val);
        List<String> res = tc.getExclude_apps();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setExclude_apps()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setExclude_apps(val);
        List<String> res = tc.getExclude_apps();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getPm()
    {
        TargetingChannel tc = new TargetingChannel();
        String pm = "CPC";
        tc.setPm(pm);
        TargetingChannel.PriceModel res = tc.getPm();
        assertEquals(pm, res.toString());
    }

    @Test
    public void setPm()
    {
        TargetingChannel tc = new TargetingChannel();
        String pm = "CPC";
        tc.setPm(pm);
        TargetingChannel.PriceModel res = tc.getPm();
        assertEquals(pm, res.toString());
    }

    @Test
    public void getIplCityCodes()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setIplCityCodes(val);
        List<String> res = tc.getIplCityCodes();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setIplCityCodes()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setIplCityCodes(val);
        List<String> res = tc.getIplCityCodes();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void getResidenceCityNames()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setResidenceCityNames(val);
        List<String> res = tc.getResidenceCityNames();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void setResidenceCityNames()
    {
        TargetingChannel tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setResidenceCityNames(val);
        List<String> res = tc.getResidenceCityNames();
        assertEquals(val.toString(), res.toString());
    }

    @Test
    public void convertToList()
    {
        String str = "123-456-789";
        List<String> res = TargetingChannel.convertToList(str);
        assertEquals(Arrays.asList(str).toString(), res.toString());
    }

    @Test
    public void getQueryKey()
    {
        TargetingChannel tc = new TargetingChannel();
        assertEquals("none", tc.getQueryKey());
        tc = new TargetingChannel();
        tc.setG(Arrays.asList("G"));
        tc.setA(Arrays.asList("A"));
        tc.setR(Arrays.asList("R"));
        tc.setT(Arrays.asList("T"));
        tc.setSi(Arrays.asList("Si"));
        tc.setM(Arrays.asList("M"));
        tc.setPm("CPC");
        tc.setDpc(Arrays.asList("DPC"));
        tc.setIpl(Arrays.asList("IPL"));
        tc.setAus(Arrays.asList("AUS"));
        tc.setPdas(Arrays.asList("PDAS"));
        tc.setAis(Arrays.asList("AIS"));
        tc.setDms(Arrays.asList("DMS"));
        tc.setExclude_pdas(Arrays.asList("EX_PDAS"));
        tc.setExclude_apps(Arrays.asList("EX_APPS"));
        String exp = "r|g|a|t|si|m|cpm-cpc|dpc|ipl|aus|pdas|ais|dms";
        assertEquals(exp, tc.getQueryKey());
    }

    @Test
    public void hasMultiValues()
    {
        TargetingChannel tc = new TargetingChannel();
        assertFalse(tc.hasMultiValues());
        tc = new TargetingChannel();
        tc.setG(Arrays.asList("G"));
        assertFalse(tc.hasMultiValues());
        tc.setPm("CPC");
        assertFalse(tc.hasMultiValues());
        tc.setAus(Arrays.asList("AUS"));
        assertTrue(tc.hasMultiValues());
    }

    @Test
    public void testToString()
    {
        TargetingChannel tc = new TargetingChannel();
        assertEquals("none", tc.getQueryKey());
        tc = new TargetingChannel();
        tc.setG(Arrays.asList("G"));
        tc.setA(Arrays.asList("A"));
        tc.setR(Arrays.asList("R"));
        tc.setT(Arrays.asList("T"));
        tc.setSi(Arrays.asList("Si"));
        tc.setM(Arrays.asList("M"));
        tc.setPm("CPC");
        tc.setDpc(Arrays.asList("DPC"));
        tc.setIpl(Arrays.asList("IPL"));
        tc.setAus(Arrays.asList("AUS"));
        tc.setPdas(Arrays.asList("PDAS"));
        tc.setAis(Arrays.asList("AIS"));
        tc.setDms(Arrays.asList("DMS"));
        tc.setExclude_pdas(Arrays.asList("EX_PDAS"));
        tc.setExclude_apps(Arrays.asList("EX_APPS"));

        String exp = "{adv_type:[M],slot_id:[Si],net_type:[T],gender:[G],age:[A],device price category:[DPC],price_type:CPC,residence_city[R],ip_city_code[IPL],app interests[AIS],app usages[AUS],pdas[PDAS],exclude_pdas[EX_PDAS],device models[DMS]}";
        assertEquals(exp, tc.toString());
    }
}