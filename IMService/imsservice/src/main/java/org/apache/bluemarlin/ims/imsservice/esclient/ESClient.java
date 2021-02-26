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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.json.JSONException;

import java.io.IOException;

/**
 * This interface defines methods that are used by DAO layer to access and manipulate data
 * in elasticsearch.
 *
 */
public interface ESClient
{
    /**
     * This methods returns result of search query on elasticsearch.
     *
     * @param searchRequest
     * @return
     * @throws ElasticsearchException
     * @throws JSONException
     * @throws IOException
     */
    ESResponse search(SearchRequest searchRequest) throws ElasticsearchException, JSONException, IOException;

    /**
     *  This methods returns result of update query on elasticsearch.
     *
     * @param updateRequest
     * @param refreshPolicy
     * @return
     * @throws IOException
     */
    ESResponse update(UpdateRequest updateRequest, String refreshPolicy) throws IOException;

    /**
     *  This methods returns result of search-scroll query on elasticsearch. search-scroll is used to read
     *  huge set of data.
     *
     * @param searchScrollRequest
     * @return
     * @throws IOException
     * @throws JSONException
     */
    ESResponse searchScroll(SearchScrollRequest searchScrollRequest) throws IOException, JSONException;

    /**
     *  This methods returns result of add query on elasticsearch.
     *
     * @param indexRequest
     * @param refreshPolicy
     * @return
     * @throws IOException
     */
    IndexResponse index(IndexRequest indexRequest, String refreshPolicy) throws IOException;

    /**
     *  This methods returns true if a result of delete request is successful.
     *
     * @param deleteRequest
     * @return
     * @throws IOException
     */
    boolean delete(DeleteRequest deleteRequest) throws IOException;
}
