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

package com.bluemarlin.ims.imsservice.model;

import com.bluemarlin.ims.imsservice.util.CommonUtil;
import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class TargetingChannel implements Serializable
{
    private static final long serialVersionUID = -6898810772165687122L;

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

    @JsonProperty("aus")
    private List<String> aus;

    @JsonProperty("ais")
    private List<String> ais;

    @JsonProperty("pdas")
    private List<String> pdas;

    @JsonProperty("exclude_pdas")
    private List<String> exclude_pdas;

    @JsonProperty("apps")
    private List<String> apps;

    @JsonProperty("exclude_apps")
    private List<String> exclude_apps;

    @JsonProperty("dms")
    private List<String> dms;

    @JsonProperty("pm")
    @JsonAlias({"priceModel"})
    private PriceModel pm = PriceModel.NONE;

    @JsonProperty("dpc")
    @JsonAlias({"devicePriceCat"})
    private List<String> dpc;

    @JsonProperty("ipl")
    @JsonAlias({"ipLocation"})
    private List<String> ipl;

    @JsonIgnore
    private List<String> iplCityCodes;

    @JsonIgnore
    private List<String> residenceCityNames;

    public static List<String> addAllAttributeValues(List<TargetingChannel> tcs, String attributeName)
    {
        Set<String> rset = new HashSet<>();
        for (TargetingChannel tc : tcs)
        {
            List<String> attributeValues = tc.getAttributeValue(attributeName);
            if (attributeValues != null)
            {
                rset.addAll(attributeValues);
            }
        }
        return new ArrayList<>(rset);
    }

    public static TargetingChannel build(Map<String, Object> map)
    {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> mapClone = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet())
        {
            Object value = entry.getValue();
            String key = entry.getKey();
            if (value == null || String.valueOf(value).equals("null"))
            {
                continue;
            }
            if (!key.equals("priceModel") && !key.equals("pm"))
            {
                if (value instanceof Collection)
                {
                    mapClone.put(key, value);
                }
                else
                {
                    List<String> newValue = new ArrayList<>(Arrays.asList((String.valueOf(value))));
                    mapClone.put(key, newValue);
                }
            }
            else
            {
                mapClone.put(key, value);
            }
        }
        TargetingChannel result = mapper.convertValue(mapClone, TargetingChannel.class);
        return result;
    }

    public List<String> getAttributeValue(String attributeName)
    {
        switch (attributeName)
        {
            case "age":
                return this.getA();
            case "gender":
                return this.getG();
            case "media":
                return this.getM();
            case "si":
                return this.getSi();
            case "connectionType":
                return this.getT();
            default:
                break;
        }
        return new ArrayList<>();
    }

    public void setAttributeValue(String attributeName, Object value)
    {
        switch (attributeName)
        {
            case "a":
                this.setA((List<String>) value);
                break;
            case "g":
                this.setG((List<String>) value);
                break;
            default:
                break;
        }
    }

    public List<String> getAis()
    {
        return ais;
    }

    public void setAis(List<String> ais)
    {
        this.ais = CommonUtil.removeStringFromItems(ais, "-");
    }

    public List<String> getPdas()
    {
        return pdas;
    }

    public void setPreDefinedAudience(List<String> preDefinedAudience)
    {
        this.pdas = preDefinedAudience;
    }

    public List<String> getDms()
    {
        return dms;
    }

    public void setDeviceModel(List<String> deviceModel)
    {
        this.dms = deviceModel;
    }

    public List<String> getAus()
    {
        return aus;
    }

    public void setAus(List<String> appUsage)
    {
        this.aus = CommonUtil.removeStringFromItems(appUsage, "-");
    }

    public void setDms(List<String> dms)
    {
        this.dms = CommonUtil.removeStringFromItems(dms, "-");
    }

    public void setPdas(List<String> pdas)
    {
        this.pdas = pdas;
    }

    public List<String> getR()
    {
        return r;
    }

    public void setR(List<String> r)
    {
        this.r = r;
    }

    public List<String> getG()
    {
        return g;
    }

    public void setG(List<String> g)
    {
        this.g = g;
    }

    public List<String> getA()
    {
        return a;
    }

    public void setA(List<String> a)
    {
        this.a = a;
    }

    public List<String> getT()
    {
        return t;
    }

    public void setT(List<String> t)
    {
        this.t = t;
    }

    public List<String> getSi()
    {
        return si;
    }

    public void setSi(List<String> si)
    {
        this.si = si;
    }

    public List<String> getM()
    {
        return m;
    }

    public void setM(List<String> m)
    {
        this.m = m;
    }

    public List<String> getDpc()
    {
        return dpc;
    }

    public void setDpc(List<String> dpc)
    {
        this.dpc = dpc;
    }

    public List<String> getIpl()
    {
        return ipl;
    }

    public void setIpl(List<String> ipl)
    {
        this.ipl = ipl;
    }

    public List<String> getExclude_pdas()
    {
        return exclude_pdas;
    }

    public void setExclude_pdas(List<String> exclude_pdas)
    {
        this.exclude_pdas = exclude_pdas;
    }

    public List<String> getApps()
    {
        return this.apps;
    }

    public void setApps(List<String> apps)
    {
        this.apps = CommonUtil.removeStringFromItems(apps, "-");
    }

    public List<String> getExclude_apps()
    {
        return this.exclude_apps;
    }

    public void setExclude_apps(List<String> exclude_apps)
    {
        this.exclude_apps = exclude_apps;
    }

    public PriceModel getPm()
    {
        return pm;
    }

    public void setPm(String pm)
    {
        if (!CommonUtil.isBlank(pm))
        {
            this.pm = PriceModel.valueOf(pm.toUpperCase(Locale.US));
        }
    }

    public List<String> getIplCityCodes()
    {
        return iplCityCodes;
    }

    public void setIplCityCodes(List<String> iplCityCodes)
    {
        this.iplCityCodes = iplCityCodes;
    }

    public List<String> getResidenceCityNames()
    {
        return residenceCityNames;
    }

    public void setResidenceCityNames(List<String> residenceCityNames)
    {
        this.residenceCityNames = residenceCityNames;
    }

    public static List<String> convertToList(String v)
    {
        List<String> result = new ArrayList<>();
        if (v != null)
        {
            result.add(v);
        }
        return result;
    }

    @JsonIgnore
    public String getQueryKey()
    {
        StringBuilder result = new StringBuilder();
        if (!CommonUtil.isEmpty(r))
        {
            result.append(CommonUtil.convertToString(r));
            result.append("|");
        }
        if (g != null)
        {
            result.append(CommonUtil.convertToString(g));
            result.append("|");
        }
        if (a != null)
        {
            result.append(CommonUtil.convertToString(a));
            result.append("|");
        }
        if (t != null)
        {
            result.append(CommonUtil.convertToString(t));
            result.append("|");
        }
        if (si != null)
        {
            result.append(CommonUtil.convertToString(si));
            result.append("|");
        }
        if (m != null)
        {
            result.append(CommonUtil.convertToString(m));
            result.append("|");
        }
        if (pm != null)
        {
            result.append(CommonUtil.sanitize(pm.getCPCCPMName().toLowerCase()));
            result.append("|");
        }
        if (dpc != null)
        {
            result.append(CommonUtil.convertToString(dpc));
            result.append("|");
        }
        if (ipl != null)
        {
            result.append(CommonUtil.convertToString(ipl));
            result.append("|");
        }
        if (!CommonUtil.isEmpty(aus))
        {
            result.append(CommonUtil.convertToString(aus));
            result.append("|");
        }
        if (!CommonUtil.isEmpty(pdas))
        {
            result.append(CommonUtil.convertToString(pdas));
            result.append("|");
        }
        if (!CommonUtil.isEmpty(ais))
        {
            result.append(CommonUtil.convertToString(ais));
            result.append("|");
        }
        if (!CommonUtil.isEmpty(dms))
        {
            result.append(CommonUtil.convertToString(dms));
            result.append("|");
        }
        if (result.length() > 0)
        {
            result.delete(result.length() - 1, result.length());
        }
        return result.toString();
    }

    public boolean hasMultiValues()
    {
        if (!CommonUtil.isEmpty(aus) || !CommonUtil.isEmpty(pdas) || !CommonUtil.isEmpty(exclude_pdas)
                || !CommonUtil.isEmpty(ais) || !CommonUtil.isEmpty(dms) || !CommonUtil.isEmpty(apps)
                || !CommonUtil.isEmpty(exclude_apps) || !CommonUtil.isEmpty(dpc))
        {
            return true;
        }

        return false;
    }

    @Override
    public String toString()
    {
        String result = "{";
        if (m != null)
        {
            result += "adv_type:" + m + ",";
        }
        if (si != null)
        {
            result += "slot_id:" + si + ",";
        }
        if (t != null)
        {
            result += "net_type:" + t + ",";
        }
        if (g != null)
        {
            result += "gender:" + g + ",";
        }
        if (a != null)
        {
            result += "age:" + a + ",";
        }
        if (dpc != null)
        {
            result += "device price category:" + dpc + ",";
        }
        if (pm != null)
        {
            result += "price_type:" + pm + ",";
        }
        if (r != null)
        {
            result += "residence_city" + r + ",";
        }
        if (ipl != null)
        {
            result += "ip_city_code" + ipl + ",";
        }
        if (!CommonUtil.isEmpty(ais))
        {
            result += "app interests" + ais + ",";
        }
        if (!CommonUtil.isEmpty(aus))
        {
            result += "app usages" + aus + ",";
        }
        if (!CommonUtil.isEmpty(pdas))
        {
            result += "pdas" + pdas + ",";
        }
        if (!CommonUtil.isEmpty(exclude_pdas))
        {
            result += "exclude_pdas" + exclude_pdas + ",";
        }
        if (!CommonUtil.isEmpty(dms))
        {
            result += "device models" + dms;
        }
        result += "}";
        return result;
    }

}
