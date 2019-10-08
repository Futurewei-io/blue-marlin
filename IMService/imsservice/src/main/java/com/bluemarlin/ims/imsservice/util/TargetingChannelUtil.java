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

package com.bluemarlin.ims.imsservice.util;

import com.bluemarlin.ims.imsservice.model.TargetingChannel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TargetingChannelUtil
{
    static String[] singleValueAttributes = new String[]{"age", "gender", "si", "connectionType", "media"};

    private TargetingChannelUtil()
    {
        throw new IllegalStateException("This is utility class.");
    }

    /**
     * This method returns true if any 2 of the targeting channels have intersections.
     *
     * @param targetingChannels
     * @return
     */
    public static boolean hasIntersectionsForSingleAttributes(List<TargetingChannel> targetingChannels)
    {
        for (String attributeName : singleValueAttributes)
        {
            Set<String> set = new HashSet();
            for (TargetingChannel tc : targetingChannels)
            {
                List<String> attributeValues = tc.getAttributeValue(attributeName);
                if (CommonUtil.isEmpty(attributeValues))
                {
                    continue;
                }

                if (set.size() == 0)
                {
                    set.addAll(attributeValues);
                }
                else
                {
                    set.retainAll(attributeValues);
                    if (set.size() == 0)
                    {
                        return false;
                    }
                }

            }
        }
        return true;
    }

}
