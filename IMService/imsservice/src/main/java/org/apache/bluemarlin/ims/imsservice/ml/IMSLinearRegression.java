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

package org.apache.bluemarlin.ims.imsservice.ml;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.List;


public class IMSLinearRegression
{
    private IMSLinearRegression()
    {
        throw new IllegalStateException("This is utility class.");
    }

    public static double predict(List<Long> input, int numberOfLastItems)
    {
        double[] _input = new double[numberOfLastItems];
        for (int i = _input.length - numberOfLastItems; i < _input.length; i++)
        {
            _input[i] = input.get(i);
        }
        return predict(_input);
    }

    public static double predict(double[] input)
    {
        SimpleRegression simpleRegression = new SimpleRegression();
        double[][] data = new double[input.length][2];
        for (int i = 0; i < input.length; i++)
        {
            data[i][0] = i;
            data[i][1] = input[i];
        }
        simpleRegression.addData(data);
        double r = simpleRegression.predict(input.length);
        return r;
    }

    public static double average(List<Long> input, int numberOfLastItems)
    {
        double sum = 0;
        for (int i = input.size() - numberOfLastItems; i < input.size(); i++)
        {
            sum += input.get(i);
        }
        return sum / numberOfLastItems;
    }

}
