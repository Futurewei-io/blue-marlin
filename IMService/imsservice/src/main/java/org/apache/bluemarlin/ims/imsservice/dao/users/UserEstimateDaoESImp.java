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

package org.apache.bluemarlin.ims.imsservice.dao.users;

import org.apache.bluemarlin.ims.imsservice.esclient.ESResponse;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.apache.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.bluemarlin.ims.imsservice.dao.BaseDaoESImp;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class UserEstimateDaoESImp extends BaseDaoESImp implements UserEstimateDao
{
    private static final String DAYS = "days.";

    public UserEstimateDaoESImp(Properties properties)
    {
        super(properties);
    }

    private void appendQueryForAttributeOnDays(List<String> days, String attribute, List<String> values, BoolQueryBuilder boolQueryBuilder, boolean must)
    {
        if (CommonUtil.isEmpty(values))
        {
            return;
        }

        BoolQueryBuilder query = new BoolQueryBuilder();
        for (String day : days)
        {
            String field = DAYS + day + "." + attribute;
            for (String item : values)
            {
                MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                query = query.should(_query);
            }
        }
        if (must)
        {
            boolQueryBuilder.must(query);
        }
        else
        {
            boolQueryBuilder.mustNot(query);
        }
    }

    private void appendQueryForIPLAndResidenceOnDays(List<String> days, List<String> ipls, List<String> residences, BoolQueryBuilder boolQueryBuilder)
    {
        BoolQueryBuilder query = new BoolQueryBuilder();

        if (!CommonUtil.isEmpty(ipls))
        {
            for (String day : days)
            {
                String field = DAYS + day + "." + "ipl";
                for (String item : ipls)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
            }
        }

        if (!CommonUtil.isEmpty(residences))
        {
            for (String day : days)
            {
                String field = DAYS + day + "." + "r";
                for (String item : residences)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
            }
        }
        boolQueryBuilder.must(query);
    }

    /**
     * This method build the query to count number of unique users.
     * @param days
     * @param targetingChannel
     * @return
     */
    private BoolQueryBuilder createQueryForUserEstimate(List<String> days, TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        appendQueryForAttributeOnDays(days, "t", targetingChannel.getT(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "si", targetingChannel.getSi(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "m", targetingChannel.getM(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "a", targetingChannel.getA(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "g", targetingChannel.getG(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "dpc", targetingChannel.getDpc(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "au", targetingChannel.getAus(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "ai", targetingChannel.getAis(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "dm", targetingChannel.getDms(), boolQueryBuilder, true);
        appendQueryForAttributeOnDays(days, "pda", targetingChannel.getPdas(), boolQueryBuilder, true);

        /**
         * For excluded pdas
         */
        appendQueryForAttributeOnDays(days, "pda", targetingChannel.getExclude_pdas(), boolQueryBuilder, false);

        /**
         * For city residence and ipl
         */
        appendQueryForIPLAndResidenceOnDays(days, targetingChannel.getIpl(), targetingChannel.getResidenceCityNames(), boolQueryBuilder);

        return boolQueryBuilder;
    }

    @Override
    public long getUserCount(TargetingChannel targetingChannel, List<String> days, double price) throws IOException
    {
        BoolQueryBuilder boolQueryBuilder = createQueryForUserEstimate(days, targetingChannel);
        SearchRequest searchRequest = new SearchRequest(userPredictionsIndex).source(
                new SearchSourceBuilder().query(boolQueryBuilder).size(0));

        ESResponse esResponse = esclient.search(searchRequest);
        return esResponse.getSearchHits().totalHits;
    }
}
