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

package org.apache.bluemarlin.ims.imsservice.util;

import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class TestTargetingChannelUtil
{


    @Test
    public void hasIntersectionsForSingleAttributes()
    {
        TargetingChannel tc = new TargetingChannel(), tc2 = new TargetingChannel(), tc3 = new TargetingChannel();
        tc.setG(Arrays.asList("g_m"));
        tc2.setG(Arrays.asList("g_m"));
        tc2.setDms(Arrays.asList("rneal00"));

        List<TargetingChannel> list = new ArrayList();
        boolean res = TargetingChannelUtil.hasIntersectionsForSingleAttributes(list);
        assertSame(res, true);

        list.add(tc);
        list.add(tc2);
        res = TargetingChannelUtil.hasIntersectionsForSingleAttributes(list);
        assertSame(res, true);

        list.add(tc);
        list.add(tc2);
        list.add(tc3);
        res = TargetingChannelUtil.hasIntersectionsForSingleAttributes(list);
        assertSame(res, true);

        list = new ArrayList();
        list.add(tc3);
        res = TargetingChannelUtil.hasIntersectionsForSingleAttributes(list);
        assertSame(res, true);

        list = new ArrayList();
        tc3.setA(Arrays.asList("3"));
        list.add(tc3);
        list.add(tc);
        res = TargetingChannelUtil.hasIntersectionsForSingleAttributes(list);
        assertSame(res, true);
    }
}