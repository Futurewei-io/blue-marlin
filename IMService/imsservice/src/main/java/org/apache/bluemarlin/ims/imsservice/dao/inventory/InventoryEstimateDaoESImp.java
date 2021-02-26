/**
 * Copyright 2019, Futurewei Technologies
 * <p>
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.bluemarlin.ims.imsservice.dao.inventory;

import org.apache.bluemarlin.ims.imsservice.esclient.ESResponse;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.dao.BaseDaoESImp;
import org.apache.bluemarlin.ims.imsservice.dao.util.QueryUtil;
import org.apache.bluemarlin.ims.imsservice.model.Day;
import org.apache.bluemarlin.ims.imsservice.model.DayImpression;
import org.apache.bluemarlin.ims.imsservice.model.H0;
import org.apache.bluemarlin.ims.imsservice.model.H1;
import org.apache.bluemarlin.ims.imsservice.model.H2;
import org.apache.bluemarlin.ims.imsservice.model.H3;
import org.apache.bluemarlin.ims.imsservice.model.Impression;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.apache.commons.lang3.math.NumberUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONException;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Repository
public class InventoryEstimateDaoESImp extends BaseDaoESImp implements InventoryEstimateDao
{
    private static final List<String> PRICE_CATEGORY_LIST = Arrays.asList("h0", "h1", "h2", "h3");

    public InventoryEstimateDaoESImp()
    {

    }

    public InventoryEstimateDaoESImp(Properties properties)
    {
        super(properties);
    }

    /**
     * Returns day and the its counts for [h0,h1,h2,h3]
     *
     * @param boolQueryBuilder
     * @param days
     * @return
     * @throws IOException
     * @throws JSONException
     */
    private Map<Day, Impression> getImpressionCountForFullDays(BoolQueryBuilder boolQueryBuilder,
                                                               Set<String> days) throws IOException
    {
        SearchRequest searchRequest;
        Map<Day, Impression> result = new HashMap<>();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(0);

        for (String day : days)
        {
            for (int i = 0; i < 4; i++)
            {
                SumAggregationBuilder aggregationBuilders =
                        new SumAggregationBuilder("h" + i + "_" + day).field(String.format(esPredictionsInventoryPath, day) + "h" + i);
                sourceBuilder = sourceBuilder.aggregation(aggregationBuilders);
            }
        }
        searchRequest = new SearchRequest(this.predictionsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);
        if (esResponse == null)
        {
            LOGGER.error("response is null");
            return Collections.emptyMap();
        }

        //esResponse.getSourceMap().get("sum#h1_2018-11-07")
        Map<String, Object> map = esResponse.getSourceMap();
        for (String dayStr : days)
        {
            double[] value = new double[4];
            for (int i = 0; i < 4; i++)
            {
                String key = "sum#h" + i + "_" + dayStr;
                Double _value = (Double) ((Map) (map.get(key))).get("value");
                value[i] = _value;
            }

            Day day = new Day(dayStr);
            Impression impression = new Impression(value);
            result.put(day, impression);
        }
        return result;
    }

    private DayImpression getPredictionsChart(BoolQueryBuilder boolQueryBuilder,
                                              Double regionRatio) throws IOException
    {

        DayImpression dayImpression = new DayImpression();
        List<Impression> listOfImpressions;
        SearchRequest searchRequest;
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder).size(0);

        for (int i = 0; i < 24; i++)
        {
            for (String priceCat : PRICE_CATEGORY_LIST)
            {
                String key = "hr" + i + "." + priceCat;
                String field = ES_CHART_DOC_PREFIX + "hours." + key;
                SumAggregationBuilder aggregationBuilders = new SumAggregationBuilder(key).field(field);
                sourceBuilder = sourceBuilder.aggregation(aggregationBuilders);
            }
        }

        searchRequest = new SearchRequest(this.predictionsIndex).source(sourceBuilder);
        ESResponse esResponse = esclient.search(searchRequest);
        if (esResponse == null)
        {
            LOGGER.error("response is null");
            return dayImpression;
        }

        Map<String, Object> map = QueryUtil.toMap(QueryUtil.extractAggregationStr(esResponse.getSearchResponse().toString()));

        listOfImpressions = new ArrayList<>();
        for (int i = 0; i < 24; i++)
        {
            Impression impression = new Impression();
            impression.setH_index(i);

            for (String priceCat : PRICE_CATEGORY_LIST)
            {
                String key = "hr" + Integer.toString(i) + "." + priceCat;
                Map<String, Object> valueMap = (Map<String, Object>) map.get(key);
                Double value = NumberUtils.toDouble(valueMap.get("value").toString());

                switch (priceCat)
                {
                    case "h0":
                        impression.setH0(new H0(value));
                        break;
                    case "h1":
                        impression.setH1(new H1(value));
                        break;
                    case "h2":
                        impression.setH2(new H2(value));
                        break;
                    case "h3":
                        impression.setH3(new H3(value));
                        break;
                    default:
                }
            }
            listOfImpressions.add(impression);
        }
        dayImpression.setHours(listOfImpressions);

        if (regionRatio != 1)
        {
            dayImpression = DayImpression.multiply(dayImpression, regionRatio);
        }
        return dayImpression;
    }

    private Map<Day, Impression> applyRegionRatio(Map<Day, Impression> totalMap, Map<Day, Impression> countMap,
                                                  double ratio)
    {
        Map<Day, Impression> result = new HashMap<>();
        for (Map.Entry<Day, Impression> entry : countMap.entrySet())
        {
            Day day = entry.getKey();
            Impression impression = entry.getValue();
            Day newDay = new Day(day.toString());
            Impression newImpression = Impression.multiply(impression, ratio);
            if (totalMap != null && totalMap.containsKey(day))
            {
                Impression totalDayImpression = totalMap.get(day);
                newImpression = Impression.add(totalDayImpression, newImpression);
            }
            result.put(newDay, newImpression);
        }
        return result;
    }

    private Impression getPredictionsForQuery(Day day, BoolQueryBuilder boolQueryBuilder) throws IOException
    {
        Map<Day, Impression> countMap = getImpressionCountForFullDays(boolQueryBuilder, new HashSet<>(Arrays.asList(day.toString())));
        Impression result = new Impression();
        if (countMap.size() > 0)
        {
            result = countMap.values().iterator().next();
        }
        return result;
    }

    @Override
    public DayImpression getHourlyPredictions(TargetingChannel targetingChannel,
                                              double tbrRatio) throws IOException
    {

        DayImpression dayImpression = null;
        dayImpression = new DayImpression();

        Map<Double, BoolQueryBuilder> boolQueryBuilderRatioMap = this.createQueryForSingleValuesWithRegionRatio(targetingChannel);

        // deal with each BoolQueryBuilder
        int sequence = 0;
        for (Map.Entry<Double, BoolQueryBuilder> entry : boolQueryBuilderRatioMap.entrySet())
        {
            BoolQueryBuilder boolQueryBuilder = entry.getValue();
            Double regionRatio = entry.getKey();
            LOGGER.debug("ratio={}", regionRatio);
            LOGGER.debug("querybuilder={}", boolQueryBuilder);

            if (sequence == 0)
            {
                dayImpression = getPredictionsChart(boolQueryBuilder, regionRatio);
            }
            else
            {
                dayImpression = DayImpression.add(dayImpression, getPredictionsChart(boolQueryBuilder, regionRatio));
            }
            sequence++;
        }

        // multiply TBR ratio
        if (tbrRatio != 1)
        {
            dayImpression = DayImpression.multiply(dayImpression, tbrRatio);
        }

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String today = simpleDateFormat.format(new Date());
        dayImpression.setDate(today);

        return dayImpression;
    }

    @Override
    public Impression getPredictions_SIGMA_TCs_PI_BNs(Day day, List<TargetingChannel> tcs, List<TargetingChannel> bns) throws IOException
    {
        /**
         * If tcs size is zero the return is zero
         */
        if (CommonUtil.isEmpty(tcs))
        {
            return new Impression();
        }

        BoolQueryBuilder bnsq = createAndQueryForSingleValue(bns);
        BoolQueryBuilder tcsbq = createOrQueryForSingleValue(tcs);
        bnsq = bnsq.must(tcsbq);

        return getPredictionsForQuery(day, bnsq);
    }

    @Override
    public Impression getPredictions_PI_TCs_DOT_qBAR(Day day, List<TargetingChannel> tcs, TargetingChannel q) throws IOException
    {
        BoolQueryBuilder pitcsqb = createAndQueryForSingleValue(tcs);
        BoolQueryBuilder qbar = createInvQueryForSingleValue(q);
        pitcsqb = pitcsqb.must(qbar);

        return getPredictionsForQuery(day, pitcsqb);
    }

    @Override
    public Impression getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q(Day day, List<TargetingChannel> bns, List<TargetingChannel> bms, TargetingChannel q) throws IOException
    {
        /**
         * If tcs size is zero the return is zero
         */
        if (CommonUtil.isEmpty(bns))
        {
            return new Impression();
        }

        BoolQueryBuilder bnsqb = createAndQueryForSingleValue(bns);
        BoolQueryBuilder bmsqb = createOrQueryForSingleValue(bms);
        bnsqb = bnsqb.mustNot(bmsqb);
        BoolQueryBuilder qb = createQueryForSingleValues(q);
        bnsqb = bnsqb.must(qb);

        return getPredictionsForQuery(day, bnsqb);
    }

    @Override
    public Impression getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs(Day day, List<TargetingChannel> bms, TargetingChannel q, List<TargetingChannel> bns) throws IOException
    {
        /**
         * If tcs size is zero the return is zero
         */
        if (CommonUtil.isEmpty(bns))
        {
            return new Impression();
        }

        BoolQueryBuilder bmsqb = createOrQueryForSingleValue(bms);
        BoolQueryBuilder qb = createInvQueryForSingleValue(q);
        bmsqb = bmsqb.should(qb);
        BoolQueryBuilder bnsqb = createAndQueryForSingleValue(bns);
        bnsqb = bnsqb.must(bmsqb);

        return getPredictionsForQuery(day, bnsqb);
    }

    @Override
    public Impression getPredictions_PI_TCs(Day day, List<TargetingChannel> tcs) throws IOException, JSONException
    {
        if (CommonUtil.isEmpty(tcs))
        {
            return new Impression();
        }
        BoolQueryBuilder boolQueryBuilder = createAndQueryForSingleValue(tcs);
        return getPredictionsForQuery(day, boolQueryBuilder);
    }

    /**
     * This method return the total day impressions for each day, it considers region factors.
     *
     * @param targetingChannel
     * @param days
     * @return
     * @throws JSONException
     * @throws IOException
     */
    @Override
    public Map<Day, Impression> aggregatePredictionsFullDaysWithRegionRatio(TargetingChannel targetingChannel,
                                                                            Set<Day> days) throws IOException
    {
        Map<Day, Impression> result = new HashMap<>();
        Map<Double, BoolQueryBuilder> boolQueryBuilderRatioMap = this.createQueryForSingleValuesWithRegionRatio(targetingChannel);
        for (Map.Entry<Double, BoolQueryBuilder> entry : boolQueryBuilderRatioMap.entrySet())
        {
            BoolQueryBuilder query = entry.getValue();
            Double regionRatio = entry.getKey();
            Map<Day, Impression> countMap = getImpressionCountForFullDays(query, Day.getDayString(days));
            countMap = applyRegionRatio(null, countMap, regionRatio);
            for (Map.Entry<Day, Impression> entry1 : countMap.entrySet())
            {
                Day _day = entry1.getKey();
                Impression _impression = entry1.getValue();
                if (result.containsKey(_day))
                {
                    Impression newImpression = Impression.add(_impression, result.get(_day));
                    result.put(_day, newImpression);
                }
                else
                {
                    result.put(_day, _impression);
                }
            }
        }
        return result;
    }

    @Override
    public Map<Day, Impression> aggregatePredictionsFullDays(TargetingChannel targetingChannel, Set<Day> days) throws IOException
    {
        BoolQueryBuilder query = this.createQueryForSingleValues(targetingChannel);
        Map<Day, Impression> result = getImpressionCountForFullDays(query, Day.getDayString(days));
        return result;
    }

}
