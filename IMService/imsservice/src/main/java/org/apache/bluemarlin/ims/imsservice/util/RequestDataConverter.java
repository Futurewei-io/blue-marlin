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

package org.apache.bluemarlin.ims.imsservice.util;

import org.apache.bluemarlin.ims.imsservice.model.TargetingChannel;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

/**
 * This class is used when data has city names instead of region.
 */
public class RequestDataConverter
{
    private static final String CITY_REGION_FILE_PATH = "data/dummy_city_region.txt";
    private static Map<String, List<Object>> CITY_CODE_REGION_MAP = new HashMap<>();
    private static Map<String, String> CITY_NAME_CITY_CODE_MAP = new HashMap<>();
    private static List<Double> TRAFFIC_ARRAY = new ArrayList<Double>();
    private static final IMSLogger LOGGER = IMSLogger.instance();

    static
    {
        URL url = RequestDataConverter.class.getClassLoader().getResource(CITY_REGION_FILE_PATH);
        if (url != null)
        {
            String path = url.getPath();
            if (path != null)
            {
                File file = new File(path);
                Map<String, Double> regionPopulationMap = new HashMap<>();
                Map<String, String[]> cityRegionMap = new HashMap<>();
                try (Scanner sc = new Scanner(file, "GBK");)
                {
                    while (sc.hasNextLine())
                    {
                        String line = sc.nextLine();
                        String[] parts = line.split("\\,");
                        String cityName = parts[1];
                        String cityCode = parts[0];
                        String region = parts[2];
                        String population = parts[3];

                        cityRegionMap.put(cityCode, parts);
                        if (regionPopulationMap.containsKey(region))
                        {
                            regionPopulationMap.put(region,
                                    regionPopulationMap.get(region) + Double.valueOf(population));
                        }
                        else
                        {
                            regionPopulationMap.put(region, Double.valueOf(population));
                        }

                        CITY_NAME_CITY_CODE_MAP.put(cityName, cityCode);
                    }
                    for (Map.Entry<String, String[]> e : cityRegionMap.entrySet())
                    {
                        String cityCode = e.getKey();
                        String region = e.getValue()[2];
                        String population = e.getValue()[3];
                        double overallPopulation = regionPopulationMap.get(region);
                        double ratio = Double.parseDouble(population) / overallPopulation;
                        List _list = new ArrayList();
                        _list.add(region);
                        _list.add(ratio);
                        CITY_CODE_REGION_MAP.put(cityCode, _list);
                    }
                }
                catch (FileNotFoundException e)
                {
                    LOGGER.error("Fail to get CITY_REGION file");
                }
            }
        }

        ClassLoader classLoader = RequestDataConverter.class.getClassLoader();
        if (classLoader != null)
        {
            InputStream input = classLoader.getResourceAsStream("db.properties");
            if (input == null)
            {
                LOGGER.error("Input is null");
            }

            Properties properties = new Properties();
            String[] arrayTemp;
            try
            {
                properties.load(input);
            }
            catch (IOException e)
            {
                LOGGER.error(e.getMessage());
            }
            finally
            {
                try
                {
                    input.close();
                }
                catch (IOException e)
                {
                    LOGGER.error(e.getMessage());
                }
            }

            String traffic = properties.getProperty("traffic.array");
            arrayTemp = traffic.split(",");
            for (int i = 0; i < arrayTemp.length; i++)
            {
                TRAFFIC_ARRAY.add(Double.valueOf(arrayTemp[i]));
            }
        }
    }

    private RequestDataConverter()
    {
        throw new IllegalStateException("This is utility class.");
    }

    public static String getRegionByCityCode(String cityCode)
    {
        if (CITY_CODE_REGION_MAP.containsKey(cityCode))
        {
            return String.valueOf(CITY_CODE_REGION_MAP.get(cityCode).get(0));
        }

        return "";
    }

    public static String getRegionByCityName(String cityName)
    {
        if (CITY_NAME_CITY_CODE_MAP.containsKey(cityName))
        {
            String cityCode = String.valueOf(CITY_NAME_CITY_CODE_MAP.get(cityName));
            return getRegionByCityCode(cityCode);
        }

        return "";
    }

    public static String getCityCodeByCityName(String cityName)
    {
        if (CITY_NAME_CITY_CODE_MAP.containsKey(cityName))
        {
            String cityCode = String.valueOf(CITY_NAME_CITY_CODE_MAP.get(cityName));
            return cityCode;
        }
        return "";
    }

    public static Double getRegionRatioByCityCode(String cityCode)
    {
        if (CITY_CODE_REGION_MAP.containsKey(cityCode))
        {
            return (Double) (CITY_CODE_REGION_MAP.get(cityCode).get(1));
        }

        return 0.0;
    }

    public static Map<String, Double> getRegionRatioMap(TargetingChannel targetingChannel)
    {
        Map<String, Double> regionRatioMap = new HashMap<>();
        if (!CommonUtil.isEmpty(targetingChannel.getIplCityCodes()))
        {
            for (String iplCityCode : targetingChannel.getIplCityCodes())
            {
                Double ratio = getRegionRatioByCityCode(iplCityCode);
                String region = getRegionByCityCode(iplCityCode);
                regionRatioMap.put(region, ratio);
            }
        }
        else if (!CommonUtil.isEmpty(targetingChannel.getResidenceCityNames()))
        {
            for (String residenceCityName : targetingChannel.getResidenceCityNames())
            {
                String cityCode = getCityCodeByCityName(residenceCityName);
                Double ratio = getRegionRatioByCityCode(cityCode);
                String region = getRegionByCityCode(cityCode);
                regionRatioMap.put(region, ratio);
            }
        }
        return regionRatioMap;
    }

    public static double getHourDayRatio(List<Integer> listOfHours)
    {
        if (listOfHours.size() == 24)
        {
            return 1;
        }

        double result = 0d;
        for (Integer i : listOfHours)
        {
            result = result + TRAFFIC_ARRAY.get(i);
        }
        return result / 100;
    }

    public static Map<Double, List<String>> buildRatioCityCodeRegionMap(List<String> iplCityCodes)
    {
        /**
         * It groups the regions of CityCodes with the same regional ratios together.
         * The ratio is key and the list is the cityCodes regions.
         *
         * Implement and use the 4 methods above to make things easier
         */
        Map<String, Double> regionRatioMap = new HashMap<>();
        for (String ipCityCode : iplCityCodes)
        {
            Double ratio = getRegionRatioByCityCode(ipCityCode);
            String region = getRegionByCityCode(ipCityCode);
            if (regionRatioMap.containsKey(region))
            {
                regionRatioMap.put(region, regionRatioMap.get(region) + ratio);
            }
            else
            {
                regionRatioMap.put(region, ratio);
            }
        }
        Map<Double, List<String>> ratioCityCodeRegionMap = new HashMap<>();
        for (Map.Entry<String, Double> entry : regionRatioMap.entrySet())
        {
            if (ratioCityCodeRegionMap.containsKey(entry.getValue()))
            {
                ratioCityCodeRegionMap.get(entry.getValue()).add(entry.getKey());
            }
            else
            {
                List<String> regionList = new ArrayList();
                regionList.add(entry.getKey());
                ratioCityCodeRegionMap.put(entry.getValue(), regionList);
            }

        }

        return ratioCityCodeRegionMap;
    }

    public static Map<Double, List<String>> buildRatioResidenceRegionMap(List<String> residenceCityNames)
    {
        /**
         * It groups the regions of CityName with the same regional ratios together.
         * The ratio is key and the list is the cityNames regions.
         */
        Map<String, Double> regionRatioMap = new HashMap<>();
        for (String residenceCityName : residenceCityNames)
        {
            String cityCode = getCityCodeByCityName(residenceCityName);
            Double ratio = getRegionRatioByCityCode(cityCode);
            String region = getRegionByCityCode(cityCode);
            if (regionRatioMap.containsKey(region))
            {
                regionRatioMap.put(region, regionRatioMap.get(region) + ratio);
            }
            else
            {
                regionRatioMap.put(region, ratio);
            }
        }
        Map<Double, List<String>> ratioResidenceRegionMap = new HashMap<>();
        for (Map.Entry<String, Double> entry : regionRatioMap.entrySet())
        {
            if (ratioResidenceRegionMap.containsKey(entry.getValue()))
            {
                ratioResidenceRegionMap.get(entry.getValue()).add(entry.getKey());
            }
            else
            {
                List<String> regionList = new ArrayList<String>();
                regionList.add(entry.getKey());
                ratioResidenceRegionMap.put(entry.getValue(), regionList);
            }

        }
        return ratioResidenceRegionMap;
    }
}
