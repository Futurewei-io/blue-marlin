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

package com.bluemarlin.pfservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Range
{
    @JsonProperty("st")
    private String st;

    @JsonProperty("ed")
    private String ed;

    @JsonProperty("eh")
    private String eh;

    @JsonProperty("sh")
    private String sh;

    public String getSt()
    {
        return st;
    }

    public String getEd()
    {
        return ed;
    }

    public String getEh()
    {
        return eh;
    }

    public String getSh()
    {
        return sh;
    }
}
