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

package com.bluemarlin.ims.imsservice.dao.users;

import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.json.JSONException;

import java.io.IOException;
import java.util.List;

public interface UserEstimateDao
{

    /**
     * This method returns the number of unique users in a period of time (days) that fall into the targeting channel.
     *
     * @param targetingChannel
     * @param days
     * @param price
     * @return
     * @throws IOException
     * @throws JSONException
     */
    long getUserCount(TargetingChannel targetingChannel, List<String> days, double price) throws IOException;

}
