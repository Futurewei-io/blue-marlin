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

package org.apache.bluemarlin.ims.imsservice.ml;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestIMSLinearRegression
{

    @Test
    public void predict()
    {
        List<Long> input = new ArrayList();
        input.add(123L);
        input.add(456L);
        input.add(1L);
        input.add(10L);
        int num = 2;
        double res = IMSLinearRegression.predict(input, num);
        assertEquals(789D, res, 0);
    }

    @Test
    public void testPredict()
    {
        double[] input = new double[]{1.1, 2.2, 3.3, 4.4, 500};
        double[] input2 = new double[]{1, 20, 300, 4000, 50000};
        double res = IMSLinearRegression.predict(input);
        double res2 = IMSLinearRegression.predict(input2);
        assertEquals(402.2, res, 0.1D);
        assertEquals(42057.6D, res2, 0.1D);
    }

    @Test
    public void average()
    {
        List<Long> input = new ArrayList();
        input.add(123L);
        input.add(456L);
        input.add(1L);
        input.add(10L);
        input.add(1000000L);
        int num = 3;
        double res = IMSLinearRegression.average(input, num);
        double exp = 1000011 / 3;
        assertEquals(exp, res, 0);
    }
}