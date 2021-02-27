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

package org.apache.bluemarlin.ims.imsservice.dao;

import org.apache.bluemarlin.ims.imsservice.dao.booking.TestBookingDaoESImp;
import org.apache.bluemarlin.ims.imsservice.esclient.ESClient;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class TestBaseDaoESImp {

    static InputStream DEF_INPUT = TestBookingDaoESImp.class.getClassLoader().getResourceAsStream("db-test.properties");
    static Properties DEF_PROP;

    static {
        DEF_PROP = new Properties();
        try {
            DEF_PROP.load(DEF_INPUT);
        } catch (
                IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testInit() {
        BaseDaoESImp bd = new BaseDaoESImp(DEF_PROP);
        assertNotNull(bd);
    }

    @Test
    public void getResourceAbsolutePath() {
        String path = "";
        String res = BaseDaoESImp.getResourceAbsolutePath(path);
        String exp = "\\IMService\\imsservice\\build\\classes\\java\\test";
        assertTrue(res.endsWith(exp));
    }

    @Test
    public void genericScriptReader() throws URISyntaxException {
        String absPath = BaseDaoESImp.getResourceAbsolutePath("es/TbrKeysTotal.txt");
        String res = BaseDaoESImp.genericScriptReader(Paths.get(absPath));
        String exp = "int totals=0;for(int entry : params._source.data.days.values()){totals+=entry;}return totals;";
        assertTrue(res.equals(exp));
    }

    @Test
    public void setESClient() {
        BaseDaoESImp bd = new BaseDaoESImp();
        ESClient esClient = null;
        bd.setESClient(esClient);
        assertNull(bd.esclient);
    }

    @Test
    public void append() {
        BoolQueryBuilder bqBldr = new BoolQueryBuilder();
        List<String> list = new ArrayList();
        String field = "field";
        BaseDaoESImp bd = new BaseDaoESImp();
        bd.append(bqBldr, list, field);
        bd.append(bqBldr, list, new ArrayList(), field);
        bd.append(bqBldr, field, "value");

        String exp = "{\n" +
                "  \"bool\" : {\n" +
                "    \"must\" : [\n" +
                "      {\n" +
                "        \"match_phrase\" : {\n" +
                "          \"field\" : {\n" +
                "            \"query\" : \"value\",\n" +
                "            \"slop\" : 0,\n" +
                "            \"zero_terms_query\" : \"NONE\",\n" +
                "            \"boost\" : 1.0\n" +
                "          }\n" +
                "        }\n" +
                "      }\n" +
                "    ],\n" +
                "    \"adjust_pure_negative\" : true,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        assertTrue(exp.equals(bqBldr.toString()));
    }

    @Test
    public void appendBase() {
        BoolQueryBuilder bqBldr = new BoolQueryBuilder();
        List<String> list = new ArrayList(), noList = new ArrayList();
        list.add("pdas");
        noList.add("uninstalled");
        String field = "field";
        BaseDaoESImp bd = new BaseDaoESImp();
        bd.appendBase(bqBldr, list, noList, field);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"field\":{\"query\":\"pdas\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}},"
                + "{\"match_phrase\":{\"field\":{\"query\":\"uninstalled\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"must_not\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"field\":{\"query\":\"pdas\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}},"
                + "{\"match_phrase\":{\"field\":{\"query\":\"uninstalled\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, bqBldr.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createQueryForMultiValue() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        List<String> val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setAus(val);
        BoolQueryBuilder res = bd.createQueryForMultiValue(tc);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"au\":{\"query\":\"123\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}},"
                + "{\"match_phrase\":{\"au\":{\"query\":\"456\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}},"
                + "{\"match_phrase\":{\"au\":{\"query\":\"789\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createQueryForMultiValuePlusAgeGender() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        BoolQueryBuilder res = bd.createQueryForMultiValuePlusAgeGender(tc);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"data.g\":{\"query\":\"g_f\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createQueryForAgeGender() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        BoolQueryBuilder res = bd.createQueryForAgeGender(tc);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"data.a\":{\"query\":\"3\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}},{\"bool\":{\"should\":[{\"match_phrase\":{\"data.g\":{\"query\":\"g_f\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createQueryForSingleValuesWithRegionRatio() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        tc.setA(Arrays.asList("3"));
        Map<Double, BoolQueryBuilder> res = bd.createQueryForSingleValuesWithRegionRatio(tc);
        assertNotNull(res);

        tc = new TargetingChannel();
        List<String> val = new ArrayList();
        val.add("city1");
        val.add("city2");
        val.add("city3");
        tc.setResidenceCityNames(val);
        res = bd.createQueryForSingleValuesWithRegionRatio(tc);
        assertNotNull(res);

        tc = new TargetingChannel();
        val = new ArrayList();
        val.add("123");
        val.add("456");
        val.add("7-8-9");
        tc.setIplCityCodes(val);
        res = bd.createQueryForSingleValuesWithRegionRatio(tc);
        assertNotNull(res);
    }

    @Test
    public void createQueryForSingleValues() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        BoolQueryBuilder res = bd.createQueryForSingleValues(tc);
        String exp = "{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createOrQueryForSingleValue() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        tc.setG(Arrays.asList("g_f"));
        List<TargetingChannel> list = new ArrayList();
        list.add(tc);
        BoolQueryBuilder res = bd.createOrQueryForSingleValue(list);
        String exp = "{\"bool\":{\"should\":[{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}},{\"bool\":{\"should\":[{\"match_phrase\":{\"ucdoc.g\":{\"query\":\"g_f\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createInvQueryForSingleValue() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        BoolQueryBuilder res = bd.createInvQueryForSingleValue(tc);
        String exp = "{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}}],\"must_not\":[{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createInvQuery() {
        BaseDaoESImp bd = new BaseDaoESImp();
        BoolQueryBuilder bqBldr = new BoolQueryBuilder();
        BoolQueryBuilder res = bd.createInvQuery(bqBldr);
        String exp = "{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}}],\"must_not\":[{\"bool\":{\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createAndQueryForSingleValue() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        List<TargetingChannel> list = new ArrayList();
        list.add(tc);
        TargetingChannel tc2 = new TargetingChannel();
        tc2.setG(Arrays.asList("G"));
        list.add(tc2);
        BoolQueryBuilder res = bd.createAndQueryForSingleValue(list);
        String exp = "{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}},{\"bool\":{\"must\":[{\"match_all\":{\"boost\":1.0}},{\"bool\":{\"should\":[{\"match_phrase\":{\"ucdoc.g\":{\"query\":\"G\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createAndQueryForMultiValuePlusAgeGender() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        List<TargetingChannel> list = new ArrayList();
        list.add(tc);
        TargetingChannel tc2 = new TargetingChannel();
        tc2.setG(Arrays.asList("G"));
        list.add(tc2);
        BoolQueryBuilder res = bd.createAndQueryForMultiValuePlusAgeGender(list);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"data.g\":{\"query\":\"G\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }

    @Test
    public void createAndQueryForAgeGender() {
        BaseDaoESImp bd = new BaseDaoESImp();
        TargetingChannel tc = new TargetingChannel();
        List<TargetingChannel> list = new ArrayList();
        list.add(tc);
        TargetingChannel tc2 = new TargetingChannel();
        tc2.setG(Arrays.asList("G"));
        list.add(tc2);
        BoolQueryBuilder res = bd.createAndQueryForAgeGender(list);
        String exp = "{\"bool\":{\"must\":[{\"bool\":{\"must\":[{\"bool\":{\"should\":[{\"match_phrase\":{\"data.g\":{\"query\":\"G\",\"slop\":0,\"zero_terms_query\":\"NONE\",\"boost\":1.0}}}],"
                + "\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}],\"adjust_pure_negative\":true,\"boost\":1.0}}";
        assertEquals(exp, res.toString().replaceAll("\\s+", ""));
    }
}