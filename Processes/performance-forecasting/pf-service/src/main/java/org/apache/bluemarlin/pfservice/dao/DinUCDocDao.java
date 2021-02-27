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

package org.apache.bluemarlin.pfservice.dao;

import org.apache.bluemarlin.pfservice.config.SystemConfig;
import org.apache.bluemarlin.pfservice.model.Day;
import org.apache.bluemarlin.pfservice.model.GraphRequest;
import org.apache.bluemarlin.pfservice.model.Impression;
import org.apache.bluemarlin.pfservice.util.CommonUtil;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class DinUCDocDao extends BaseESDao
{
    @Autowired
    @Qualifier("RestHighLevelClient")
    private RestHighLevelClient restHighLevelClient;

    @Autowired
    @Qualifier("SystemConfig")
    SystemConfig systemConfig;

    private String aggScript;

    public DinUCDocDao() throws IOException
    {
    }

    public Impression getImpression(GraphRequest graphRequest, int gte, int lte) throws Exception
    {
        if (aggScript == null)
        {
            aggScript = getResourceContent("file:src/main/resources/agg-script.txt");
        }
        BoolQueryBuilder boolQueryBuilder = this.createBaseQueryForSingleValues(graphRequest.getTraffic());
        if (!CommonUtil.isBlank(graphRequest.getKeyword()))
        {
            String keywordField = String.format("%s.%s.%s", ES_KEYWORD_DOC_PREFIX, graphRequest.getKeyword(), ES_KEYWORD_B_PREFIX);
            RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder(keywordField).gte(gte).lte(lte);
            boolQueryBuilder.must(rangeQueryBuilder);
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(0);
        List<Day> days = Day.buildSortedDays(graphRequest.getRanges());

        for (int i = 0; i < 4; i++)
        {
            List<String> dayParams = new ArrayList<>();
            for (Day day : days)
            {
                String param = String.format("inventory.%s.h%d", day.toString(), i);
                dayParams.add(param);
            }
            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("days", dayParams);
            Script script = new Script(ScriptType.INLINE, "painless", aggScript, scriptParams);
            SumAggregationBuilder aggregationBuilders =
                    new SumAggregationBuilder("h" + i).script(script);
            sourceBuilder = sourceBuilder.aggregation(aggregationBuilders);
        }

        String dinUCDocIndex = this.systemConfig.getDinUCDocESIndex();
        SearchRequest searchRequest = new SearchRequest(dinUCDocIndex).source(sourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        if (searchResponse == null)
        {
            LOGGER.error("response is null");
            throw new Exception("");
        }

        Map<String, Aggregation> map = searchResponse.getAggregations().getAsMap();
        return new Impression(map);
    }
}
