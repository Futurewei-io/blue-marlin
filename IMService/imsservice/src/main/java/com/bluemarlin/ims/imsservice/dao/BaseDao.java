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

import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.List;
import java.util.Map;

public interface BaseDao
{

    /**
     * This method returns a query to retrieve data sets that match single-value attributes of targeting channel.
     * This method considers region ratio and the output is an organized map of region ration and query.
     *
     * @param targetingChannel
     * @return
     */
    Map<Double, BoolQueryBuilder> createQueryForSingleValuesWithRegionRatio(TargetingChannel targetingChannel);

    /**
     * This method returns a query to retrieve data sets that match single-value attributes of targeting channel.
     *
     * @param targetingChannel
     * @return
     */
    BoolQueryBuilder createQueryForSingleValues(TargetingChannel targetingChannel);

    /**
     * This method returns a query to retrieve data sets that match multi-value attributes of targeting channel.
     *
     * @param targetingChannel
     * @return
     */
    BoolQueryBuilder createQueryForMultiValue(TargetingChannel targetingChannel);

    /**
     * This method returns a query to retrieve data sets that match multi-value attributes of targeting channel
     * plus age and gender.
     *
     * @param targetingChannel
     * @return
     */
    BoolQueryBuilder createQueryForMultiValuePlusAgeGender(TargetingChannel targetingChannel);

    /**
     * This method returns a query to retrieve data sets that match age and gender of targeting channel.
     *
     * @param targetingChannel
     * @return
     */
    BoolQueryBuilder createQueryForAgeGender(TargetingChannel targetingChannel);

    /**
     * This method returns a query to retrieve data sets that match single-value attributes of
     * any targeting channel of a collection.
     *
     * @param tcs
     * @return
     */
    BoolQueryBuilder createOrQueryForSingleValue(List<TargetingChannel> tcs);

    /**
     * This method returns a query to retrieve data sets that do not match single-value attributes of targeting channel.
     *
     * @param targetingChannel
     * @return
     */
    BoolQueryBuilder createInvQueryForSingleValue(TargetingChannel targetingChannel);

    /**
     * This method inverts a query.
     *
     * @param boolQueryBuilder
     * @return
     */
    BoolQueryBuilder createInvQuery(BoolQueryBuilder boolQueryBuilder);

    /**
     * This method returns a query to retrieve data sets that match single-value attributes of
     * every targeting channel of a collection.
     *
     * @param tcs
     * @return
     */
    BoolQueryBuilder createAndQueryForSingleValue(List<TargetingChannel> tcs);

    /**
     * This method returns a query to retrieve data sets that match multi-value attributes of
     * every targeting channel of a collection.
     *
     * @param tcs
     * @return
     */
    BoolQueryBuilder createAndQueryForMultiValuePlusAgeGender(List<TargetingChannel> tcs);

    /**
     *  This method returns a query to retrieve data sets that match age and gender attributes of
     *  every targeting channel of a collection.
     *
     * @param tcs
     * @return
     */
    BoolQueryBuilder createAndQueryForAgeGender(List<TargetingChannel> tcs);
}
