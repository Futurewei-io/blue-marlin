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

package com.bluemarlin.ims.imsservice.dao.tbr;

import com.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;

public interface TBRDao
{
    /**
     * This method returns tbr ration for a targeting channel.
     * @param targetingChannel
     * @return
     * @throws JSONException
     * @throws ESConnectionException
     */
    double getTBRRatio(TargetingChannel targetingChannel) throws ESConnectionException;

    /**
     * This method returns tbr ratio of the following complex query
     * (TC1 and TC2 and TC3 ...)
     * @param tcs
     * @return
     * @throws IOException
     * @throws JSONException
     */
    double getTBRRatio_PI_TCs(List<TargetingChannel> tcs) throws IOException;

    /**
     * This method returns tbr ratio of the following complex query
     * (Bn1 and Bn2 and ...) and (~q)
     * @param bns
     * @param q
     * @return
     * @throws IOException
     * @throws JSONException
     */
    double getTBRRatio_PI_BNs_DOT_qBAR(List<TargetingChannel> bns, TargetingChannel q) throws IOException;
}
