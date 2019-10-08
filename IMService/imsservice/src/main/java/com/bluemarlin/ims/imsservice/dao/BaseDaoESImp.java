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

package com.bluemarlin.ims.imsservice.dao;

import com.bluemarlin.ims.imsservice.esclient.ESClient;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import com.bluemarlin.ims.imsservice.util.IMSLogger;
import com.bluemarlin.ims.imsservice.util.RequestDataConverter;
import org.apache.commons.codec.Charsets;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

@Component
public class BaseDaoESImp implements BaseDao
{
    protected final String TBR_KEYS_TOTAL = getResourceAbsolutePath("es/TbrKeysTotal.txt");

    public static final String ES_TYPE = "doc";
    public static final String ES_PREDICTION_DOC_PREFIX = "ucdoc.";
    public static final String ES_TBR_DOC_PREFIX = "data.";
    public static final String ES_CHART_DOC_PREFIX = ES_PREDICTION_DOC_PREFIX + "predictions_chart.";

    protected String tbrIndex = "";
    protected String predictionsIndex = "";
    protected String bookingsIndex = "";
    protected String userPredictionsIndex = "";
    protected String bookingBucketsIndex = "";

    protected static final IMSLogger LOGGER = IMSLogger.instance();
    protected Properties properties;

    @Autowired
    protected ESClient esclient;

    public static String getResourceAbsolutePath(String resourcePath)
    {
        String path = BaseDao.class.getClassLoader().getResource(resourcePath).getPath();
        File file = new File(path);
        return file.getAbsolutePath();
    }

    public static String genericScriptReader(Path path)
    {
        String fileContents = "";
        try
        {
            List<String> file = java.nio.file.Files.readAllLines(path,
                    Charsets.UTF_8);
            for (String s : file)
            {
                fileContents += s.trim();
            }
        }
        catch (IOException e)
        {
            LOGGER.error(e.getMessage());
        }
        return fileContents;
    }

    public BaseDaoESImp()
    {
    }

    public BaseDaoESImp(Properties properties)
    {
        this.properties = properties;
        tbrIndex = properties.getProperty("es.tbr.index");
        predictionsIndex = properties.getProperty("es.predictions.index");
        bookingsIndex = properties.getProperty("es.bookings.index");
        bookingBucketsIndex = properties.getProperty("es.booking_buckets.index");
        userPredictionsIndex = properties.getProperty("es.user_predictions.index");
    }

    public void setESClient(ESClient esclient)
    {
        this.esclient = esclient;
    }

    /**
     * This method returns the base query (only single-value attributes) for a targeting channel.
     * @param targetingChannel
     * @return
     */
    private BoolQueryBuilder createBaseQueryForSingleValues(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.must(new MatchAllQueryBuilder());

        append(boolQueryBuilder, targetingChannel.getT(), ES_PREDICTION_DOC_PREFIX + "t");
        append(boolQueryBuilder, targetingChannel.getSi(), ES_PREDICTION_DOC_PREFIX + "si");
        append(boolQueryBuilder, targetingChannel.getM(), ES_PREDICTION_DOC_PREFIX + "m");
        append(boolQueryBuilder, targetingChannel.getA(), ES_PREDICTION_DOC_PREFIX + "a");
        append(boolQueryBuilder, targetingChannel.getG(), ES_PREDICTION_DOC_PREFIX + "g");
        return boolQueryBuilder;
    }

    /**
     * This method adds an attribute with its possible values to a query as Must operand.
     *
     * @param boolQueryBuilder : query that new attribute appends to
     * @param list : list of values
     * @param field : attribute name
     */
    protected void append(BoolQueryBuilder boolQueryBuilder, List<String> list, String field)
    {
        appendBase(boolQueryBuilder, list, null, field);
    }

    /**
     * This method adds an attribute with its value to a query.
     *
     * @param boolQueryBuilder
     * @param key
     * @param value
     */
    protected void append(BoolQueryBuilder boolQueryBuilder, String key, Object value)
    {
        MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(key, value);
        boolQueryBuilder.must(_query);
    }

    /**
     * This method adds an attribute with its possible values to a query as Must or notMust operand.
     *
     * If the values are in List then the operand is Must.
     * If the values are in noList then the operand is notMust.
     *
     * @param boolQueryBuilder
     * @param list
     * @param notList
     * @param field
     */
    protected void append(BoolQueryBuilder boolQueryBuilder, List<String> list, List<String> notList, String field)
    {
        appendBase(boolQueryBuilder, list, notList, field);
    }

    /**
     *  This method adds an attribute with its possible values to a query as Must or notMust operand.
     *
     *  If the values are in List then the operand is Must.
     *  If the values are in noList then the operand is notMust.
     *
     * @param boolQueryBuilder
     * @param list
     * @param notList
     * @param field
     */
    protected void appendBase(BoolQueryBuilder boolQueryBuilder, List<String> list, List<String> notList, String field)
    {
        //single choice in inapp: pdas or exclude_pdas, installed or uninstalled
        if (!CommonUtil.isEmpty(list) || !CommonUtil.isEmpty(notList))
        {
            BoolQueryBuilder query = new BoolQueryBuilder();
            if (list != null) //deal with pdas/installed
            {
                for (String item : list)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
                boolQueryBuilder.must(query);
            }

            if (notList != null) //deal with exclude_pdas/uninstalled
            {
                for (String item : notList)
                {
                    MatchPhraseQueryBuilder _query = new MatchPhraseQueryBuilder(field, item);
                    query = query.should(_query);
                }
                boolQueryBuilder.mustNot(query);
            }
        }
    }

    @Override
    public BoolQueryBuilder createQueryForMultiValue(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        /**
         * App usage has the part to hand different app states, such as installed, active and etc...
         */
        if (!CommonUtil.isEmpty(targetingChannel.getAus()))
        {
            /**
             * Split app usage into 2 parts, not installed and the rest
             */
            List<String> notInstalledApps = new ArrayList<>();
            List<String> restApps = new ArrayList<>();
            for (String appUsage : targetingChannel.getAus())
            {
                if (appUsage.length() > 0)
                {
                    if (appUsage.toLowerCase(Locale.US).contains("not_installed"))
                    {
                        /**
                         * Just get the app name not the postfixes
                         */
                        notInstalledApps.add(appUsage.split("_")[0] + "_type_installed");
                    }
                    else
                    {
                        restApps.add(appUsage);
                    }
                }
            }
            if (CommonUtil.isEmpty(notInstalledApps))
            {
                notInstalledApps = null;
            }
            if (CommonUtil.isEmpty(restApps))
            {
                restApps = null;
            }
            append(boolQueryBuilder, restApps, notInstalledApps, "au");
        }

        append(boolQueryBuilder, targetingChannel.getPdas(), targetingChannel.getExclude_pdas(), ES_TBR_DOC_PREFIX + "pda");
        append(boolQueryBuilder, targetingChannel.getApps(), targetingChannel.getExclude_apps(), ES_TBR_DOC_PREFIX + "app");
        append(boolQueryBuilder, targetingChannel.getDms(), ES_TBR_DOC_PREFIX + "dm");
        append(boolQueryBuilder, targetingChannel.getAis(), ES_TBR_DOC_PREFIX + "ai");
        append(boolQueryBuilder, targetingChannel.getDpc(), ES_TBR_DOC_PREFIX + "dpc");

        return boolQueryBuilder;
    }

    @Override
    public BoolQueryBuilder createQueryForMultiValuePlusAgeGender(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = createQueryForMultiValue(targetingChannel);
        append(boolQueryBuilder, targetingChannel.getA(), ES_TBR_DOC_PREFIX + "a");
        append(boolQueryBuilder, targetingChannel.getG(), ES_TBR_DOC_PREFIX + "g");
        return boolQueryBuilder;
    }

    @Override
    public BoolQueryBuilder createQueryForAgeGender(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        append(boolQueryBuilder, targetingChannel.getA(), ES_TBR_DOC_PREFIX + "a");
        append(boolQueryBuilder, targetingChannel.getG(), ES_TBR_DOC_PREFIX + "g");
        return boolQueryBuilder;
    }

    @Override
    public Map<Double, BoolQueryBuilder> createQueryForSingleValuesWithRegionRatio(TargetingChannel targetingChannel)
    {

        Map<Double, BoolQueryBuilder> boolQueryBuilderRatioMap = new HashMap<>();
        if (!CommonUtil.isEmpty(targetingChannel.getResidenceCityNames()))
        {
            Map<Double, List<String>> ratioResidenceRegionMap =
                    RequestDataConverter.buildRatioResidenceRegionMap(targetingChannel.getResidenceCityNames());
            for (Map.Entry<Double, List<String>> entry : ratioResidenceRegionMap.entrySet())
            {
                BoolQueryBuilder boolQueryBuilder = createBaseQueryForSingleValues(targetingChannel);
                BoolQueryBuilder rsQuery = new BoolQueryBuilder();
                append(rsQuery, entry.getValue(), ES_PREDICTION_DOC_PREFIX + "r");
                boolQueryBuilder.must(rsQuery);
                boolQueryBuilderRatioMap.put(entry.getKey(), boolQueryBuilder);
            }
        }
        else if (!CommonUtil.isEmpty(targetingChannel.getIplCityCodes()))
        {
            Map<Double, List<String>> ratioCityCodeRegionMap =
                    RequestDataConverter.buildRatioCityCodeRegionMap(targetingChannel.getIplCityCodes());
            for (Map.Entry<Double, List<String>> entry : ratioCityCodeRegionMap.entrySet())
            {
                BoolQueryBuilder boolQueryBuilder = createBaseQueryForSingleValues(targetingChannel);
                BoolQueryBuilder riplQuery = new BoolQueryBuilder();
                append(riplQuery, entry.getValue(), ES_PREDICTION_DOC_PREFIX + "ipl");
                boolQueryBuilder.must(riplQuery);
                boolQueryBuilderRatioMap.put(entry.getKey(), boolQueryBuilder);
            }
        }
        else
        {
            BoolQueryBuilder boolQueryBuilder = createBaseQueryForSingleValues(targetingChannel);
            boolQueryBuilderRatioMap.put(1.0, boolQueryBuilder);
        }
        Iterator<Double> iter = boolQueryBuilderRatioMap.keySet().iterator();
        while (iter.hasNext())
        {
            double key = iter.next();
            if (Math.abs(key) < 0.00001)
            {
                iter.remove();
            }
        }

        return boolQueryBuilderRatioMap;
    }

    @Override
    public BoolQueryBuilder createQueryForSingleValues(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder boolQueryBuilder = createBaseQueryForSingleValues(targetingChannel);
        append(boolQueryBuilder, targetingChannel.getIpl(), ES_PREDICTION_DOC_PREFIX + "ipl");
        append(boolQueryBuilder, targetingChannel.getR(), ES_PREDICTION_DOC_PREFIX + "r");
        return boolQueryBuilder;
    }

    @Override
    public BoolQueryBuilder createOrQueryForSingleValue(List<TargetingChannel> tcs)
    {
        BoolQueryBuilder result = new BoolQueryBuilder();
        for (TargetingChannel tc : tcs)
        {
            BoolQueryBuilder boolQueryBuilder = this.createQueryForSingleValues(tc);
            result.should(boolQueryBuilder);
        }
        return result;
    }

    @Override
    public BoolQueryBuilder createInvQueryForSingleValue(TargetingChannel targetingChannel)
    {
        BoolQueryBuilder result = new BoolQueryBuilder();
        result.must(QueryBuilders.matchAllQuery());

        BoolQueryBuilder tcbq = this.createBaseQueryForSingleValues(targetingChannel);
        result.mustNot(tcbq);

        return result;
    }

    @Override
    public BoolQueryBuilder createInvQuery(BoolQueryBuilder boolQueryBuilder)
    {
        BoolQueryBuilder result = new BoolQueryBuilder();
        result.must(QueryBuilders.matchAllQuery());
        result.mustNot(boolQueryBuilder);

        return result;
    }

    @Override
    public BoolQueryBuilder createAndQueryForSingleValue(List<TargetingChannel> tcs)
    {
        BoolQueryBuilder boolQueryBuilder = null;
        for (TargetingChannel tc : tcs)
        {
            if (boolQueryBuilder == null)
            {
                boolQueryBuilder = createQueryForSingleValues(tc);
            }
            else
            {
                BoolQueryBuilder boolQueryBuilder1 = createQueryForSingleValues(tc);
                boolQueryBuilder.must(boolQueryBuilder1);
            }
        }
        return boolQueryBuilder;
    }

    @Override
    public BoolQueryBuilder createAndQueryForMultiValuePlusAgeGender(List<TargetingChannel> tcs)
    {
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
        return boolQueryBuilder;
    }

    @Override
    public BoolQueryBuilder createAndQueryForAgeGender(List<TargetingChannel> tcs)
    {
        BoolQueryBuilder boolQueryBuilder = null;
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
        return boolQueryBuilder;
    }

}

