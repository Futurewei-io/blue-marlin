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

package org.apache.bluemarlin.ims.imsservice.dao.inventory;

import org.apache.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import org.apache.bluemarlin.ims.imsservice.model.Day;
import org.apache.bluemarlin.ims.imsservice.model.DayImpression;
import org.apache.bluemarlin.ims.imsservice.model.Impression;
import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.apache.bluemarlin.ims.imsservice.dao.BaseDao;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;


public interface InventoryEstimateDao extends BaseDao
{

    /**
     * This method returns the most current hourly prediction for a targeting channel.
     *
     * @param targetingChannel
     * @param tbrRatio
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     * @throws IOException
     */
    DayImpression getHourlyPredictions(TargetingChannel targetingChannel,
                                       double tbrRatio) throws ESConnectionException, IOException;

    /**
     * This method returns the inventory result of the following query
     * TC1 and TC2 and TC3 and ... TCn
     *
     * @param day
     * @param tcs
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Impression getPredictions_PI_TCs(Day day, List<TargetingChannel> tcs) throws IOException;

    /**
     * This method returns the inventory result of the following query
     * (TC1 or TC2 or TC3 or ...) and (Bn1 and Bn2 ...)
     *
     * @param day
     * @param tcs
     * @param bns
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Impression getPredictions_SIGMA_TCs_PI_BNs(Day day, List<TargetingChannel> tcs, List<TargetingChannel> bns) throws IOException;

    /**
     * This method returns the inventory result of the following query
     * [(Bm1 or Bm2 or ...) or (~q) ] and (Bn1 and Bn2 ...)
     *
     * @param day
     * @param bms
     * @param q
     * @param bns
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Impression getPredictions_SIGMA_BMs_PLUS_qBAR_DOT_BNs(Day day, List<TargetingChannel> bms, TargetingChannel q, List<TargetingChannel> bns) throws IOException;

    /**
     * This method returns the inventory result of the following query
     * (TC1 and TC2 and ...) and (~q)
     *
     * @param day
     * @param tcs
     * @param q
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Impression getPredictions_PI_TCs_DOT_qBAR(Day day, List<TargetingChannel> tcs, TargetingChannel q) throws IOException;

    /**
     * This method returns the inventory result of the following query
     * [(Bn1 and Bn2 and ...) - (Bm1 or Bm2 ...)] and q
     *
     * @param day
     * @param bns
     * @param bms
     * @param q
     * @return
     * @throws IOException
     * @throws JSONException
     */
    Impression getPredictions_PI_BNs_MINUS_SIGMA_BMs_DOT_q(Day day, List<TargetingChannel> bns, List<TargetingChannel> bms, TargetingChannel q) throws IOException;

    /**
     * This method returns inventory for each day in an organized form.
     *
     * @param targetingChannel
     * @param days
     * @return
     * @throws JSONException
     * @throws IOException
     */
    Map<Day, Impression> aggregatePredictionsFullDays(TargetingChannel targetingChannel,
                                                      Set<Day> days) throws IOException;

    /**
     * This method returns inventory for each day in an organized form.
     * It considers region ratio.
     *
     * @param targetingChannel
     * @param days
     * @return
     * @throws JSONException
     * @throws IOException
     */
    Map<Day, Impression> aggregatePredictionsFullDaysWithRegionRatio(TargetingChannel targetingChannel,
                                                                     Set<Day> days) throws IOException;
}
