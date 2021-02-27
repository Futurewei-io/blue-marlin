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

package org.apache.bluemarlin.ims.imsservice.esclient;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.search.SearchHits;

import java.util.Map;

/**
 * This class represents the elasticsearch response.
 */
public class ESResponse
{
    private Map<String, Object> sourceMap;

    private SearchHits searchHits;

    private UpdateResponse updateResponse;

    private SearchResponse searchResponse;

    public ESResponse(SearchResponse searchResponse, Map<String, Object> map)
    {
        this.searchResponse = searchResponse;
        this.searchHits = searchResponse.getHits();
        this.sourceMap = map;
    }

    public ESResponse(UpdateResponse updateResponse)
    {
        this.updateResponse = updateResponse;
    }

    public Map<String, Object> getSourceMap()
    {
        return sourceMap;
    }

    public SearchHits getSearchHits()
    {
        return searchHits;
    }

    public SearchResponse getSearchResponse()
    {
        return searchResponse;
    }

    public UpdateResponse getUpdateResponse()
    {
        return updateResponse;
    }
}
