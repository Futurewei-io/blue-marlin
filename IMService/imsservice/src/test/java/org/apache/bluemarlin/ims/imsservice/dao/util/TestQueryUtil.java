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

package org.apache.bluemarlin.ims.imsservice.dao.util;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.suggest.Suggest;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestQueryUtil
{

    @Test
    public void extractAggregationValue()
    {
        int docId = 0, docId1 = 1, docId2 = 2, docId3 = 3, docId4 = 400, docId5 = 5000;
        SearchHit[] hit = new SearchHit[6];
        hit[0] = new SearchHit(docId);
        hit[1] = new SearchHit(docId1);
        hit[2] = new SearchHit(docId2);
        hit[3] = new SearchHit(docId3);
        hit[4] = new SearchHit(docId4);
        hit[5] = new SearchHit(docId5);
        SearchHits hits = new SearchHits(hit, 30000, 450);

        Aggregations aggregations = new Aggregations(new ArrayList());

        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions = new ArrayList();
        suggestions.add(new Suggest.Suggestion("sug1", 1));
        suggestions.add(new Suggest.Suggestion("sug2", 2));
        suggestions.add(new Suggest.Suggestion("sug3", 3));
        suggestions.add(new Suggest.Suggestion("sug4", 4));
        suggestions.add(new Suggest.Suggestion("sug5", 50));

        Suggest suggest = new Suggest(suggestions);

        SearchProfileShardResults profileResults = new SearchProfileShardResults(Collections.emptyMap());

        SearchResponseSections internalResponse = new SearchResponseSections(hits, aggregations, suggest, false, false, profileResults, 0);

        ShardSearchFailure[] shardFailures = new ShardSearchFailure[0];

        SearchResponse.Clusters clusters = SearchResponse.Clusters.EMPTY;

        SearchResponse sRes = new SearchResponse(internalResponse, "id1", 1, 1, 1, 1, shardFailures, clusters);

        JSONObject res = QueryUtil.extractAggregationValue(sRes);

        assertNotNull(res.get("suggest"));
        assertNotNull(res.get("hits"));
    }

    @Test
    public void toList()
    {
        JSONArray arr = new JSONArray();
        JSONObject obj = new JSONObject();
        obj.put("aggregations", "{\n" + "\"count\":{\n" + "\"value\" : 0.0\n" + "}\n" + "}");
        obj.put("gender", "g_f");
        obj.put("name", "obj");
        JSONObject obj2 = new JSONObject();
        obj2.put("gender", "g_f");
        obj2.put("name", "obj2");
        JSONObject obj3 = new JSONObject();
        obj3.put("aggregations", "{\n" + "\"count\":{\n" + "\"value\" : 3.0\n" + "}\n" + "}");
        obj3.put("gender", "g_n");
        obj3.put("name", "obj3");

        arr.put(obj);
        arr.put(obj2);
        arr.put(obj3);

        List res = QueryUtil.toList(arr);

        assertNotNull(res);
        assertEquals(3, res.size(), 0);
        assertEquals("{gender=g_f, name=obj2}", res.get(1).toString());
    }

    @Test
    public void toMap()
    {
        JSONObject obj = new JSONObject();
        obj.put("aggregations", "{\n" + "\"count\":{\n" + "\"value\" : 0.0\n" + "}\n" + "}");
        obj.put("gender", "g_f");
        obj.put("name", "obj");

        JSONArray arr = new JSONArray();
        JSONObject obj2 = new JSONObject();
        obj2.put("gender", "g_f");
        obj2.put("name", "obj2");

        JSONObject obj3 = new JSONObject();
        arr.put(obj2);
        obj3.put("arr", arr);

        JSONObject obj4 = new JSONObject();
        obj4.put("obj", obj2);

        Map res = QueryUtil.toMap(obj);
        assertNotNull(res);
        res = QueryUtil.toMap(obj2);
        assertEquals("{gender=g_f, name=obj2}", res.toString());
        res = QueryUtil.toMap(obj3);
        assertEquals("[{gender=g_f, name=obj2}]", res.get("arr").toString());
        res = QueryUtil.toMap(obj4);
        assertNotNull(res.get("obj"));
    }

    @Test
    public void extractAggregationStr()
    {
        String resp = "{\n" +
                "  \"took\" : 1,\n" +
                "  \"timed_out\" : false,\n" +
                "  \"_shards\" : {\n" +
                "    \"total\" : 5,\n" +
                "    \"successful\" : 5,\n" +
                "    \"skipped\" : 0,\n" +
                "    \"failed\" : 0\n" +
                "  },\n" +
                "  \"hits\" : {\n" +
                "    \"total\" : 85,\n" +
                "    \"max_score\" : 0.0,\n" +
                "    \"hits\" : [ ]\n" +
                "  },\n" +
                "  \"aggregations\" : {\n" +
                "    \"count\" : {\n" +
                "      \"value\" : 0.0\n" +
                "    }\n" +
                "  }\n" +
                "}\n";

        JSONObject res = QueryUtil.extractAggregationStr(resp);
        assertEquals("{\"value\":0}", res.get("count").toString());
    }
}