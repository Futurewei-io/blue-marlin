/**
 * Copyright 2019, Futurewei Technologies
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bluemarlin.ims.imsservice.dao.BaseDao;
import com.bluemarlin.ims.imsservice.dao.booking.BookingDaoESImp;
import com.bluemarlin.ims.imsservice.dao.inventory.InventoryEstimateDaoESImp;
import com.bluemarlin.ims.imsservice.dao.tbr.TBRDaoESImp;
import com.bluemarlin.ims.imsservice.esclient.ESClient;
import com.bluemarlin.ims.imsservice.esclient.ESRichClientImp;
import com.bluemarlin.ims.imsservice.exceptions.ESConnectionException;
import com.bluemarlin.ims.imsservice.exceptions.NoInventoryExceptions;
import com.bluemarlin.ims.imsservice.model.BookResult;
import com.bluemarlin.ims.imsservice.model.DayImpression;
import com.bluemarlin.ims.imsservice.model.IMSTimePoint;
import com.bluemarlin.ims.imsservice.model.Impression;
import com.bluemarlin.ims.imsservice.model.Range;
import com.bluemarlin.ims.imsservice.model.TargetingChannel;
import com.bluemarlin.ims.imsservice.service.BookingService;
import com.bluemarlin.ims.imsservice.service.InventoryEstimateService;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestBase
{

    protected RestHighLevelClient rhlclient;
    protected Properties properties;
    protected InventoryEstimateService inventoryEstimateService;
    protected BookingService bookingService;
    protected String file;
    private InventoryEstimateDaoESImp inventoryEstimateDao;
    private TBRDaoESImp tbrDao;
    private BookingDaoESImp bookingDao;
    private ClassLoader classLoader;
    private String bookingsIndex;
    private String bookingBucketsIndex;

    public TestBase() throws IOException
    {
        InputStream input = this.getClass().getClassLoader().getResourceAsStream("db-test.properties");
        properties = new Properties();
        properties.load(input);

        rhlclient = new RestHighLevelClient(RestClient.builder(
                new HttpHost(properties.getProperty("db.host.urls"),
                        Integer.parseInt(properties.getProperty("db.host.ports")), "http")));
        ESClient esClient = new ESRichClientImp(rhlclient);

        this.bookingsIndex = properties.getProperty("es.bookings.index");
        this.bookingBucketsIndex = properties.getProperty("es.booking_buckets.index");

        inventoryEstimateDao = new InventoryEstimateDaoESImp(properties);
        inventoryEstimateDao.setESClient(esClient);

        tbrDao = new TBRDaoESImp(properties);
        tbrDao.setESClient(esClient);

        bookingDao = new BookingDaoESImp(properties);
        bookingDao.setESClient(esClient);

        inventoryEstimateService = new InventoryEstimateService(inventoryEstimateDao, tbrDao, bookingDao);
        bookingService = new BookingService(inventoryEstimateService, inventoryEstimateDao, tbrDao, bookingDao);

        classLoader = BaseDao.class.getClassLoader();
    }

    protected BookResult putBookingEntry(TargetingChannel targetingChannel, List<Range> ranges, double price, String advId, long count) throws NoInventoryExceptions, JSONException, ESConnectionException, IOException
    {
        return null;
    }

    public File[] getFiles(String directoryPath)
    {
        File files = new File(this.getClass().getClassLoader().getResource(directoryPath).getPath());
        return files.listFiles();
    }

    public String readFile(File file) throws FileNotFoundException
    {
        Scanner sc = new Scanner(file);

        StringBuilder sb = new StringBuilder();
        while (sc.hasNextLine())
        {
            sb.append(sc.next());
        }
        return sb.toString();
    }

    private void putPredictionsDocs() throws IOException, JSONException
    {
        File[] files = getFiles("docs/predictions");
        for (File file : files)
        {
            String content = readFile(file);
            JSONObject jsonObject = new JSONObject(content);
            ObjectMapper mapper = new ObjectMapper();
            Map map = mapper.readValue(content, new TypeReference<Map<String, Object>>()
            {
            });
            rhlclient.index(new IndexRequest()
                    .index(properties.getProperty("es.predictions.index"))
                    .type(properties.getProperty("es.predictions.type"))
                    .id(jsonObject.getString("uckey")).source(map));
        }
    }

    private void putDocIntoIndex(String fileName, String index) throws IOException, JSONException
    {
        File file = new File(this.getClass().getClassLoader().getResource("docs" + fileName).getPath());
        String content = readFile(file);
        ObjectMapper mapper = new ObjectMapper();
        Map sourceMap = mapper.readValue(content, new TypeReference<Map<String, Object>>()
        {
        });
        IndexRequest indexRequest = new IndexRequest(index, "doc");
        indexRequest.source(sourceMap);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL.getValue());
        this.rhlclient.index(indexRequest);
    }

    private void putBookingsDocs() throws IOException, JSONException
    {
        File[] files = getFiles("docs/bookings");
        for (File file : files)
        {
            String content = readFile(file);
            JSONObject jsonObject = new JSONObject(content);
            ObjectMapper mapper = new ObjectMapper();
            Map map = mapper.readValue(content, new TypeReference<Map<String, Object>>()
            {
            });
            rhlclient.index(new IndexRequest()
                    .index(properties.getProperty("es.bookings.index"))
                    .type(properties.getProperty("es.bookings.type"))
                    .id(jsonObject.getString("bookingId")).source(map));
        }
    }

    protected Integer getActualImpressions(TargetingChannel tc, double price, List<Range> rangeList)
            throws JSONException, IOException, IllegalAccessException
    {
        int count = 0;
        int priceCat = CommonUtil.determinePriceCat(price, TargetingChannel.PriceModel.CPM);
        File[] files = getFiles("docs/predictions");
        boolean containsAllFields;

        //iterate each prediction doc
        for (File file : files)
        {
            String content = readFile(file);
            JSONObject predictionDoc = new JSONObject(content);
            JSONArray predictionList = (JSONArray) predictionDoc.get("predictions");

            containsAllFields = isMatch(tc, file);
            List<IMSTimePoint> imsTimePointList = IMSTimePoint.build(rangeList);
            int dateIndex = 0;
            IMSTimePoint currentDate = imsTimePointList.get(dateIndex);
            if (containsAllFields == false) continue;
            for (IMSTimePoint imsTimePoint : imsTimePointList)
            {
                if (!currentDate.getDate().equals(imsTimePoint.getDate()))
                {
                    ++dateIndex;
                    currentDate = imsTimePoint;
                }
                if (imsTimePoint.getDate().equals(predictionList.getJSONObject(dateIndex).get("date")))
                {
                    if (containsAllFields)
                    {
                        ObjectMapper mapper = new ObjectMapper();
                        DayImpression dayImpression;
                        dayImpression = mapper.
                                readValue(predictionList.get(dateIndex).toString(), new TypeReference<DayImpression>()
                                {
                                });
                        List<Impression> impressionList = dayImpression.getHours();
                        for (Impression hour : impressionList)
                        {
                            if (hour.getH_index() == imsTimePoint.getHourIndex())
                            {
                                if (priceCat == 0) count += hour.getH0().getT();
                                if (priceCat == 1) count += hour.getH1().getT();
                                if (priceCat == 2) count += hour.getH2().getT();
                                if (priceCat == 3) count += hour.getH3().getT();
                            }
                        }
                    }
                }
            }
        }
        return count;
    }

    protected Integer getTBRCount(TargetingChannel tc, boolean matchSingleValues)
            throws FileNotFoundException, JSONException, IllegalAccessException
    {
        Integer totalCount = 0;
        boolean matchedTbrDoc = false;
        File[] files = getFiles("docs/tbr");
        ObjectMapper objectMapper = new ObjectMapper();

        for (File file : files)
        {
            String content = readFile(file);
            JSONObject tbrDoc = new JSONObject(content);
            matchedTbrDoc = matchTBR(tc, file, matchSingleValues);
            JSONArray key = ((JSONObject) tbrDoc.get("days")).names();
            if (matchedTbrDoc == true)
            {
                for (int i = 0; i < key.length(); ++i)
                {
                    String keys = key.getString(i);
                    totalCount += (Integer) ((JSONObject) tbrDoc.get("days")).get(keys);
                }
            }
        }
        return totalCount;
    }

    private boolean matchTBR(TargetingChannel tc, File file, boolean matchSingleValues) throws FileNotFoundException, JSONException, IllegalAccessException
    {
        boolean ausMatch = false;
        boolean aisMatch = false;
        boolean pdasMatch = false;
        boolean dmsMatch = false;
        boolean gMatch = false;
        boolean aMatch = false;

        Field[] fields = tc.getClass().getDeclaredFields();
        String content = readFile(file);
        JSONObject tbrDoc = new JSONObject(content);
        List<String> keysList = new ArrayList<>();

        //iterate each tc fields
        for (Field field : fields)
        {
            field.setAccessible(true);

            if (field.get(tc) != null)
            {
                String fieldName = field.toString().substring(field.toString().lastIndexOf(".") + 1);
                //multi-values
                if (field.getType().equals(List.class) && !matchSingleValues)
                {
                    List<String> tcMVKList = (ArrayList<String>) field.get(tc);

                    List<String> ucKeysList;
                    //if tbr doc tc has pdas key and tbr must also have pdas key
                    if (fieldName.equals("pdas"))
                    {
                        String uckey = tbrDoc.getString("uckey");
                        ucKeysList = getUCkeyList(uckey);
                        pdasMatch = (!Collections.disjoint(ucKeysList, tcMVKList));
                        ucKeysList.clear();
                        continue;
                    }
                    if (fieldName.equals("ais"))
                    {
                        String uckey = tbrDoc.getString("uckey");
                        ucKeysList = getUCkeyList(uckey);
                        aisMatch = (!Collections.disjoint(ucKeysList, tcMVKList));
                        ucKeysList.clear();
                        continue;
                    }
                    if (fieldName.equals("aus"))
                    {
                        String uckey = tbrDoc.getString("uckey");
                        ucKeysList = getUCkeyList(uckey);
                        ausMatch = (!Collections.disjoint(ucKeysList, tcMVKList));
                        ucKeysList.clear();
                        continue;
                    }
                    if (fieldName.equals("dms"))
                    {
                        String uckey = tbrDoc.getString("uckey");
                        ucKeysList = getUCkeyList(uckey);
                        dmsMatch = (!Collections.disjoint(ucKeysList, tcMVKList));
                        ucKeysList.clear();
                        continue;
                    }

                    continue;
                }
                if (field.getType().equals(String.class) && matchSingleValues)
                {
                    if (fieldName.equals("a"))
                    {
                        if (tbrDoc.get(fieldName).toString().equals(field.get(tc)))
                        {
                            aMatch = true;
                            continue;
                        }
                        else
                        {
                            aMatch = false;
                            continue;
                        }
                    }
                    if (fieldName.equals("g"))
                    {
                        if (tbrDoc.get(fieldName).toString().equals(field.get(tc)))
                        {
                            gMatch = true;
                            continue;
                        }
                        else
                        {
                            gMatch = false;
                            continue;
                        }
                    }
                }
            }
        }
        if (matchSingleValues) return (aMatch || gMatch);
        if (tc.getPdas() == null) pdasMatch = true;
        if (tc.getAis() == null) aisMatch = true;
        if (tc.getAus() == null) ausMatch = true;
        if (tc.getDms() == null) dmsMatch = true;

        return (ausMatch && aisMatch && pdasMatch && dmsMatch);
    }

    private Stream<String> getStringStream(List<String> list)
    {
        Stream<String> stringStream = list.stream();
        return stringStream;
    }

    private List<String> getUCkeyList(String uckey)
    {
        List<String> ucKeysList = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uckey.length(); i++)
        {
            if (uckey.charAt(i) == '|')
            {
                ucKeysList.add(sb.toString());
                sb.setLength(0);
                continue;
            }
            sb.append(uckey.charAt(i));
        }
        return ucKeysList;
    }

    private boolean isMatch(TargetingChannel tc, File jsonFile) throws IllegalAccessException, JSONException, FileNotFoundException
    {
        boolean matchAnyFields = false;
        Field[] fields = tc.getClass().getDeclaredFields();
        String content = readFile(jsonFile);
        JSONObject jsonDoc = new JSONObject(content);
        List jsonObjects = new ArrayList<>();

        //iterate each tc fields
        for (Field field : fields)
        {
            //found matched prediction doc
            field.setAccessible(true);
            //single value keys
            if (field.get(tc) != null)
            {
                if (field.getType().equals(List.class))
                {
                    List tcKeys = (List) field.get(tc);
                    JSONArray multiKeysList = (JSONArray) jsonDoc.get(field.toString().substring(field.toString().lastIndexOf(".") + 1));
                    String tcKeysHyphenedJoined = tcKeys.stream().collect(Collectors.joining("|")).toString();

                    for (int i = 0; i < (multiKeysList != null ? multiKeysList.length() : 0);
                         jsonObjects.add(multiKeysList.get(i++).toString()))
                        ;

                    String docKeysHyphenedJoined = jsonObjects.stream().collect(Collectors.joining("|")).toString();
                    if (tcKeysHyphenedJoined.contains(docKeysHyphenedJoined))
                    {
                        matchAnyFields = true;
                        continue;
                    }
                    else
                    {
                        matchAnyFields = false;
                        continue;
                    }
                }
                else
                {
                    if (field.get(tc).toString().equals(
                            jsonDoc.get(field.toString().substring(field.toString().lastIndexOf(".") + 1)).toString()))
                    {
                        matchAnyFields = true;
                        continue;
                    }
                    else
                    {
                        matchAnyFields = false;
                        break;
                    }
                }
            }
        }
        return matchAnyFields;
    }

    private void putTBRDocs() throws IOException, JSONException
    {
        File[] files = getFiles("docs/tbr");
        for (File file : files)
        {
            String content = readFile(file);
            JSONObject jsonObject = new JSONObject(content);
            ObjectMapper mapper = new ObjectMapper();
            Map map = mapper.readValue(content, new TypeReference<Map<String, Object>>()
            {
            });
            rhlclient.index(new IndexRequest()
                    .index(properties.getProperty("es.tbr.index"))
                    .type(properties.getProperty("es.tbr.type"))
                    .id(jsonObject.getString("uckey")).source(map));
        }
    }

    protected List<String[]> fileToListOfRows(String fileName) throws IOException
    {
        String csvline;
        String[] csvRow;
        List<String[]> csvRowList = csvRowList = new ArrayList<>();
        try
        {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            br.readLine();
            while ((csvline = br.readLine()) != null)
            {
                csvRow = csvline.split(",");
                csvRowList.add(csvRow);
            }

        }
        catch (IOException e)
        {
            throw e;
        }
        return csvRowList;
    }

    public void runTest(String fileName) throws IOException, JSONException, ESConnectionException
    {
        /**
         * Test on file 1
         */
        file = classLoader.getResource(fileName).getPath()
                .replaceFirst("/", "").replaceFirst("%20", " ");
        List<String[]> rowList = fileToListOfRows(file);
        Integer testNum = 2;
        for (String[] row : rowList)
        {
            System.out.print("Test Case: " + testNum);
            TargetingChannel tc = (TargetingChannel) rowToParam(row, "tc");
            Double price = (Double) rowToParam(row, "price");
            List<Range> rangeList = (List<Range>) rowToParam(row, "range");
            Integer ntp = (Integer) rowToParam(row, "ntp");
            Integer tolerance = (Integer) rowToParam(row, "tolerance");
            Double ratio = (Double) rowToParam(row, "ratio");

            int priceCategory = CommonUtil.determinePriceCat(price, tc.getPm());
            long actual = getActualValue(priceCategory, ntp, ratio);

            try
            {
                getInventoryEstimateTest(tc, rangeList, price, actual, tolerance);
                System.out.println(" Passed");
            }
            catch (AssertionError e)
            {
                System.out.println(" Failed: " + e.getMessage());
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            catch (ExecutionException e)
            {
                e.printStackTrace();
            }
            ++testNum;
        }
    }

    private void getInventoryEstimateTest(TargetingChannel tc, List<Range> rangeList, Double price, Long actual, Integer tolerance) throws IOException, JSONException, ESConnectionException, ExecutionException, InterruptedException
    {
        TestInventoryEstimateService test = new TestInventoryEstimateService();
        test.testInventoryEstimate(tc, rangeList, price, actual, tolerance);
    }

    protected Object rowToParam(String[] row, String type)
    {
        String value;
        List<String> valueList;
        if (type.equals("tc"))
        {
            TargetingChannel tc = new TargetingChannel();
            valueList = (row[0].isEmpty()) ? null : Arrays.asList(new String[]{row[0]});
            tc.setR(valueList);
            value = (row[1].isEmpty()) ? null : row[1];
            tc.setA(TargetingChannel.convertToList(value));
            value = (row[2].isEmpty()) ? null : row[2];
            tc.setG(TargetingChannel.convertToList(value));
            value = (row[3].isEmpty()) ? null : row[3];
            tc.setT(TargetingChannel.convertToList(value));
            value = (row[4].isEmpty()) ? null : row[4];
            tc.setSi(TargetingChannel.convertToList(value));
            value = (row[5].isEmpty()) ? null : row[5];
            tc.setM(TargetingChannel.convertToList(value));
            value = (row[11].isEmpty()) ? null : row[11];
            tc.setPm(value);
            value = (row[12].isEmpty()) ? null : row[12];
            tc.setDpc(TargetingChannel.convertToList(value));
            value = (row[13].isEmpty()) ? null : row[13];
            tc.setDpc(TargetingChannel.convertToList(value));
            valueList = (row[7].isEmpty()) ? null : Arrays.asList(row[7].split("-"));
            tc.setAus(valueList);
            valueList = (row[8].isEmpty()) ? null : Arrays.asList(row[8].split("-"));
            tc.setAis(valueList);
            valueList = (row[9].isEmpty()) ? null : Arrays.asList(row[9].split("-"));
            tc.setPdas(valueList);
            valueList = (row[10].isEmpty()) ? null : Arrays.asList(row[10].split("-"));
            tc.setDms(valueList);
            return tc;
        }
        if (type.equals("price"))
        {
            Double price = Double.valueOf(row[6]);
            return price;
        }
        if (type.equals("range"))
        {
            List<Range> ranges = new ArrayList<>();
            Range r1 = new Range();
            r1.setSt(row[15]);
            r1.setEd(row[16]);
            r1.setSh(row[17]);
            r1.setEh(row[18]);
            ranges.add(r1);
            return ranges;
        }
        if (type.equals("ntp"))
        {
            Integer v = Integer.valueOf(row[21]);
            return v;
        }
        if (type.equals("tolerance"))
        {
            Integer v = Integer.valueOf(row[20]);
            return v;
        }
        if (type.equals("ratio"))
        {
            Double v = Double.valueOf(row[19]);
            return v;
        }
        return null;
    }

    protected long getActualValue(int hLevel, int numberOfTimePoints)
    {
        long result = 0;
        int hl = 1;
        for (int i = 1; i <= hLevel; i++)
        {
            hl += i * 100;
        }

        result = hl * numberOfTimePoints;
        return result;
    }

    protected long getActualValue(int hLevel, int numberOfTimePoints, double ratio)
    {
        long result = (long) (getActualValue(hLevel, numberOfTimePoints) * ratio);
        return result;
    }

    public void clearBookings() throws IOException, JSONException
    {
        try
        {
            this.rhlclient.indices().delete(new DeleteIndexRequest().indices(this.bookingsIndex));
            this.rhlclient.indices().delete(new DeleteIndexRequest().indices(this.bookingBucketsIndex));
        }
        catch (Exception e)
        {
            //Throws exception if the there is no index
        }

        putDocIntoIndex("/bookings/bookingSample.json", this.bookingsIndex);
        putDocIntoIndex("/bookings/bookingBucketSample.json", this.bookingBucketsIndex);
    }

    public static void main(String[] args) throws IOException, JSONException, InterruptedException, NoSuchFieldException, IllegalAccessException, InstantiationException, ClassNotFoundException
    {
        TestBase testBase = new TestBase();
        try
        {
            testBase.rhlclient.indices().delete(new DeleteIndexRequest().indices(testBase.properties.getProperty("es.predictions.index")));
            testBase.rhlclient.indices().delete(new DeleteIndexRequest().indices(testBase.properties.getProperty("es.tbr.index")));
            testBase.rhlclient.indices().delete(new DeleteIndexRequest().indices(testBase.properties.getProperty("es.bookings.index")));
            testBase.rhlclient.indices();
        }
        catch (Exception e)
        {
            /**
             * Safe exception
             */
        }

        testBase.putPredictionsDocs();
        //testBase.putBookingsDocs();
        testBase.putTBRDocs();
        testBase.rhlclient.close();
        System.exit(0);
    }
}
