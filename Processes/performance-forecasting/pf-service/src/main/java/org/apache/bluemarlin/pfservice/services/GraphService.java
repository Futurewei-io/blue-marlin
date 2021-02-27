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

package org.apache.bluemarlin.pfservice.services;

import org.apache.bluemarlin.pfservice.config.SystemConfig;
import org.apache.bluemarlin.pfservice.dao.DinUCDocDao;
import org.apache.bluemarlin.pfservice.model.GraphRequest;
import org.apache.bluemarlin.pfservice.model.GraphResponse;
import org.apache.bluemarlin.pfservice.model.Impression;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
@PropertySource("file:src/main/resources/application.properties")
public class GraphService
{
    static final int MAX_CLICK_PROBABILITY_BUCKET = 100;
    static ExecutorService executor = Executors.newFixedThreadPool(100);

    @Autowired
    DinUCDocDao dinUCDocDao;

    @Autowired
    SystemConfig systemConfig;

    public GraphResponse buildGraph(GraphRequest graphRequest) throws Exception
    {
        /**
         * Create some range according to graph size.
         * Number of max points is 100, set from inventory-updater.
         */
        int lte = MAX_CLICK_PROBABILITY_BUCKET;
        int numOfDataPoints = systemConfig.getGraphSize();
        int bucketRangeInOneCall = MAX_CLICK_PROBABILITY_BUCKET / numOfDataPoints;
        int gte = lte - bucketRangeInOneCall + 1;

        List<Future> futures = new ArrayList<>();
        List<Integer> clickProbability = new ArrayList<>();
        List<Long> inventories = new ArrayList<>();
        while (lte > 0)
        {
            final int finalLte = lte;
            final int finalGte = gte;

            Future future = executor.submit(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    Impression impression = dinUCDocDao.getImpression(graphRequest, finalGte, finalLte);
                    return impression;
                }
            });
            futures.add(future);
            clickProbability.add(lte);

            lte = gte - 1;
            gte = lte - bucketRangeInOneCall + 1;

            if (gte < 0)
            {
                gte = 0;
            }
        }

        long inventoryAmount = 0;
        for (Future future : futures)
        {
            Impression impression = (Impression) future.get();
            inventoryAmount += impression.getCountByPriceCategory(graphRequest.getPriceCategory());
            inventories.add(inventoryAmount);
        }

        return new GraphResponse(clickProbability, inventories);
    }
}
