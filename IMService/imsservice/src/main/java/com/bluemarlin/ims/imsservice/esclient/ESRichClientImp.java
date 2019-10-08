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

package com.bluemarlin.ims.imsservice.esclient;

import com.bluemarlin.ims.imsservice.util.IMSLogger;
import com.bluemarlin.ims.imsservice.dao.util.QueryUtil;
import com.bluemarlin.ims.imsservice.util.CommonUtil;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.json.JSONException;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * This class is the implementation of ESClient interface using RestHighLevelClient.
 */
public class ESRichClientImp implements ESClient
{
    protected RestHighLevelClient rhlclient;
    private static final IMSLogger LOGGER = IMSLogger.instance();

    public ESRichClientImp(Properties properties)
    {
        String[] urls = properties.getProperty("db.host.urls").split("\\,");
        String[] ports = properties.getProperty("db.host.ports").split("\\,");

        HttpHost[] httpHosts = new HttpHost[urls.length];

        for (int i = 0; i < urls.length; i++)
        {
            HttpHost httpHost = new HttpHost(urls[i], Integer.parseInt(ports[i]), "http");
            httpHosts[i] = httpHost;
        }

        rhlclient = new RestHighLevelClient(RestClient.builder(httpHosts));
    }

    public ESRichClientImp(RestHighLevelClient rhlclient)
    {
        this.rhlclient = rhlclient;
    }

    @Override
    public ESResponse search(SearchRequest searchRequest) throws ElasticsearchException, IOException, JSONException
    {
        try
        {
            SearchResponse response = rhlclient.search(searchRequest);
            LOGGER.debug("response={}", CommonUtil.removeEscapingCharacters(response.toString()));
            Map<String, Object> map = QueryUtil.toMap(QueryUtil.extractAggregationValue(response));
            return new ESResponse(response, map);
        }
        catch (ElasticsearchException e1)
        {
            throw e1;
        }
        catch (Exception e2)
        {
            throw e2;
        }
    }

    @Override
    public ESResponse update(UpdateRequest updateRequest, String refreshPolicy) throws IOException
    {
        UpdateResponse updateResponse = rhlclient.update(updateRequest);
        updateRequest.setRefreshPolicy(refreshPolicy);
        return new ESResponse(updateResponse);
    }

    @Override
    public ESResponse searchScroll(SearchScrollRequest searchScrollRequest) throws IOException, JSONException
    {
        SearchResponse response = rhlclient.searchScroll(searchScrollRequest);
        LOGGER.debug("response={}", CommonUtil.removeEscapingCharacters(response.toString()));
        Map<String, Object> map = QueryUtil.toMap(QueryUtil.extractAggregationValue(response));
        return new ESResponse(response, map);
    }

    @Override
    public IndexResponse index(IndexRequest indexRequest, String refreshPolicy) throws IOException
    {
        indexRequest.setRefreshPolicy(refreshPolicy);
        IndexResponse indexResponse = rhlclient.index(indexRequest);
        return indexResponse;
    }

    @Override
    public boolean delete(DeleteRequest deleteRequest) throws IOException
    {
        return rhlclient.delete(deleteRequest) != null;
    }
}
