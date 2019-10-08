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

package com.bluemarlin.ims.imsservice.dao.tbr;

import com.bluemarlin.ims.imsservice.esclient.ESResponse;
import com.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import com.bluemarlin.ims.imsservice.util.IMSLogger;
import com.bluemarlin.ims.imsservice.dao.BaseDaoESImp;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONException;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TBRDaoESImp extends BaseDaoESImp implements TBRDao
{
    private static final IMSLogger LOGGER = IMSLogger.instance();

    public TBRDaoESImp(Properties properties)
    {
        super(properties);
    }

    /**
     * This method returns the aggregation value of tbr documents.
     *
     * @param boolQueryBuilder
     * @return
     * @throws IOException
     * @throws JSONException
     */
    private double getSumForQuery(BoolQueryBuilder boolQueryBuilder) throws IOException
    {
        SearchRequest searchRequest;
        ESResponse esResponse;
        double ratio = 0;

        SumAggregationBuilder aggregationBuilders = new SumAggregationBuilder("sum").script(
                new Script(genericScriptReader(Paths.get(TBR_KEYS_TOTAL))));

        searchRequest = new SearchRequest(tbrIndex).source(
                new SearchSourceBuilder().query(boolQueryBuilder).size(0).
                        aggregation(aggregationBuilders));

        esResponse = esclient.search(searchRequest);
        Map<String, Object> map = esResponse.getSourceMap();

        if (map.containsKey("value"))
        {
            ratio = Double.parseDouble(map.get("value").toString());
        }
        else
        {
            LOGGER.warn("Result of getSumForQuery() does not contain key \"value!\"");
        }
        return ratio;
    }

    private BoolQueryBuilder createBoolQueryForTBR(TargetingChannel targetingChannel, boolean minusMultiValue)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        if (!minusMultiValue)
        {
            boolQueryBuilder = createQueryForMultiValue(targetingChannel);
        }

        append(boolQueryBuilder, targetingChannel.getA(), ES_TBR_DOC_PREFIX + "a");
        append(boolQueryBuilder, targetingChannel.getG(), ES_TBR_DOC_PREFIX + "g");

        return boolQueryBuilder;
    }

    @Override
    public double getTBRRatio(TargetingChannel targetingChannel) throws ESConnectionException
    {
        double ratio = 0;

        if (!targetingChannel.hasMultiValues())
        {
            return 1;
        }

        try
        {
            /**
             * Get the query count
             */
            BoolQueryBuilder boolQueryBuilder = createBoolQueryForTBR(targetingChannel, false);
            double queryCountResult = getSumForQuery(boolQueryBuilder);
            if (queryCountResult == 0)
            {
                ratio = 0;
            }
            else
            {
                /**
                 * Get the total count
                 */
                boolQueryBuilder = createBoolQueryForTBR(targetingChannel, true);
                double total = getSumForQuery(boolQueryBuilder);

                /**
                 * If total is zero, means such single value attributes do not exist, hence ratio is zero.
                 *
                 */
                if (total != 0)
                {
                    ratio = queryCountResult / total;
                }
            }
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
            throw new ESConnectionException(e.getMessage());
        }
        LOGGER.info("tbr ratio={}", ratio);
        return ratio;
    }

    @Override
    public double getTBRRatio_PI_BNs_DOT_qBAR(List<TargetingChannel> bns, TargetingChannel q) throws IOException
    {
        BoolQueryBuilder tcqb1 = createAndQueryForMultiValuePlusAgeGender(bns);
        BoolQueryBuilder tcqb2 = createQueryForMultiValuePlusAgeGender(q);
        tcqb2 = createInvQuery(tcqb2);
        tcqb1 = tcqb1.must(tcqb2);

        double v1 = getSumForQuery(tcqb1);
        if (v1 == 0)
        {
            return 0;
        }

        tcqb1 = createAndQueryForAgeGender(bns);
        tcqb2 = createQueryForAgeGender(q);
        tcqb2 = createInvQuery(tcqb2);
        tcqb1 = tcqb1.must(tcqb2);

        double v2 = getSumForQuery(tcqb1);
        if (v2 == 0)
        {
            LOGGER.info("Inconsistent data in TBR");
            v2 = 1;
        }

        return v1 / v2;
    }

    @Override
    public double getTBRRatio_PI_TCs(List<TargetingChannel> tcs) throws IOException
    {
        if (CommonUtil.isEmpty(tcs))
        {
            return 0;
        }
        BoolQueryBuilder boolQueryBuilder = null;
        for (TargetingChannel tc : tcs)
        {
            if (boolQueryBuilder == null)
            {
                boolQueryBuilder = createQueryForMultiValuePlusAgeGender(tc);
            }
            else
            {
                BoolQueryBuilder boolQueryBuilder1 = createQueryForMultiValuePlusAgeGender(tc);
                boolQueryBuilder.must(boolQueryBuilder1);
            }
        }

        double v1 = getSumForQuery(boolQueryBuilder);
        if (v1 == 0)
        {
            return 0;
        }

        boolQueryBuilder = null;
        for (TargetingChannel tc : tcs)
        {
            if (boolQueryBuilder == null)
            {
                boolQueryBuilder = createQueryForAgeGender(tc);
            }
            else
            {
                BoolQueryBuilder boolQueryBuilder1 = createQueryForAgeGender(tc);
                boolQueryBuilder.must(boolQueryBuilder1);
            }
        }

        double v2 = getSumForQuery(boolQueryBuilder);
        if (v2 == 0)
        {
            LOGGER.info("Inconsistent data in TBR");
            v2 = 1;
        }

        return v1 / v2;
    }

}
