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

package org.apache.bluemarlin.pfservice.util;

import org.elasticsearch.action.search.SearchResponse;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryUtil
{
    private static final BMLogger LOGGER = BMLogger.instance();
    private static final String VALUE_KEY = "value";

    private QueryUtil()
    {
        throw new IllegalStateException("This is utility class.");
    }

    private static JSONObject extractValueFromJSONObject(JSONObject jsonObject)
    {
        try
        {
            if (jsonObject.get(VALUE_KEY).getClass() != JSONArray.class)
            {
                Object obj = jsonObject.get(VALUE_KEY);
                if (obj instanceof JSONObject)
                {
                    jsonObject = (JSONObject) obj;
                }
            }
        }
        catch (JSONException e)
        {
            LOGGER.error(e.getMessage());
        }
        return jsonObject;
    }

    /**
     * This method converts a json object collection into its equivalent POJO.
     *
     * @param json
     * @return
     * @throws JSONException
     */
    private static Object fromJson(Object json) throws JSONException
    {
        if (json == JSONObject.NULL)
        {
            return null;
        } else if (json instanceof JSONObject)
        {
            return toMap((JSONObject) json);
        } else if (json instanceof JSONArray)
        {
            return toList((JSONArray) json);
        }

        return json;
    }

    private static JSONObject extractValueForKey(JSONObject jsonResp, String key)
    {
        try
        {
            jsonResp = (JSONObject) jsonResp.get(key);
            return jsonResp;
        }
        catch (JSONException e)
        {
            LOGGER.error(e.getMessage());
        }
        return null;
    }

    private static JSONObject getJsonObject(JSONObject jsonResp, List<String> name, Iterator keysIterator)
    {
        while (keysIterator.hasNext() && jsonResp != null)
        {
            String valueResponse = (String) keysIterator.next();
            for (String data : name)
            {
                if (valueResponse.equals(data))
                {
                    jsonResp = extractValueForKey(jsonResp, data);
                    if (jsonResp != null)
                    {
                        keysIterator = jsonResp.keys();
                    }
                    break;
                }
            }
            if (valueResponse.equals(VALUE_KEY) && jsonResp != null)
            {
                jsonResp = extractValueFromJSONObject(jsonResp);
                break;
            }
        }
        return jsonResp;
    }

    private static JSONObject keyConvert(JSONObject jsonResp, String removedPhrase)
    {
        JSONObject result = new JSONObject();
        Iterator<String> it = jsonResp.keys();
        while (it.hasNext())
        {
            String key = it.next();
            String newKey = key.replace(removedPhrase, "");
            result.put(newKey, jsonResp.get(key));
        }
        return result;
    }

    /**
     * This method extracts response from elastic search response object as a JSON Object.
     *
     * @param response
     * @return
     */
    public static JSONObject extractAggregationValue(SearchResponse response)
    {
        JSONObject jsonResp = new JSONObject();
        List<String> name = new ArrayList<>(Arrays.asList("aggregations", "sum#sum", "scripted_metric#agg"));
        try
        {
            jsonResp = new JSONObject(response.toString());
            Iterator keysIterator = jsonResp.keys();
            jsonResp = getJsonObject(jsonResp, name, keysIterator);
        }
        catch (JSONException e)
        {
            LOGGER.error(e.getMessage());
        }
        return jsonResp;
    }

    /**
     * This method converts JSON Array to ArrayList.
     *
     * @param array
     * @return
     * @throws JSONException
     */
    public static ArrayList toList(JSONArray array) throws JSONException
    {
        ArrayList list = new ArrayList();
        for (int i = 0; i < array.length(); i++)
        {
            list.add(fromJson(array.get(i)));
        }
        return list;
    }

    /**
     * This method converts a json object to a map.
     *
     * @param object
     * @return
     * @throws JSONException
     */
    public static Map<String, Object> toMap(JSONObject object) throws JSONException
    {
        Map<String, Object> map = new HashMap<>();
        Iterator<String> keysItr = object.keys();
        while (keysItr.hasNext())
        {
            String key = keysItr.next();
            Object value = object.get(key);

            if (value instanceof JSONArray)
            {
                value = toList((JSONArray) value);
            }

            if (value instanceof JSONObject)
            {
                value = toMap((JSONObject) value);
            }

            map.put(key, value);
        }
        return map;
    }

    /**
     * This method extracts aggregation response from elastic search response.
     *
     * @param response
     * @return
     */
    public static JSONObject extractAggregationStr(String response)
    {
        JSONObject jsonResp = new JSONObject();
        try
        {
            jsonResp = new JSONObject(response);
            jsonResp = extractValueForKey(jsonResp, "aggregations");
            if (jsonResp != null)
            {
                jsonResp = keyConvert(jsonResp, "sum#");
            }
        }
        catch (JSONException e)
        {
            LOGGER.error(e.getMessage());
        }
        return jsonResp;
    }

}

