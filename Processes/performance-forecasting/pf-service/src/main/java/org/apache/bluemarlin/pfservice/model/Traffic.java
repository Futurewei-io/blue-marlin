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

package org.apache.bluemarlin.pfservice.model;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class Traffic
{
    public enum PriceModel
    {
        NONE("NONE"), CPC("CPC"), CPM("CPM"), CPD("CPD"), CPT("CPT");

        private String name;

        PriceModel(String value)
        {
            name = value;
        }

        public String getCPCCPMName()
        {
            if (this == CPC || this == CPM)
            {
                return "CPM-CPC";
            }
            return name();
        }
    }

    @JsonProperty("r")
    @JsonAlias({"residence"})
    private List<String> r;

    @JsonProperty("g")
    @JsonAlias({"gender"})
    private List<String> g;

    @JsonProperty("a")
    @JsonAlias({"age"})
    private List<String> a;

    @JsonProperty("t")
    @JsonAlias({"connectionType"})
    private List<String> t;

    @JsonProperty("si")
    @JsonAlias({"slotID"})
    private List<String> si;

    @JsonProperty("m")
    @JsonAlias({"media"})
    private List<String> m;

    @JsonProperty("pm")
    @JsonAlias({"priceModel"})
    private PriceModel pm = PriceModel.NONE;

    @JsonProperty("ipl")
    @JsonAlias({"ipLocation"})
    private List<String> ipl;

    public List<String> getR()
    {
        return r;
    }

    public List<String> getG()
    {
        return g;
    }

    public List<String> getA()
    {
        return a;
    }

    public List<String> getT()
    {
        return t;
    }

    public List<String> getSi()
    {
        return si;
    }

    public List<String> getM()
    {
        return m;
    }

    public PriceModel getPm()
    {
        return pm;
    }

    public List<String> getIpl()
    {
        return ipl;
    }
}
